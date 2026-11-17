package com.amresalehin.emregallery.data

import android.content.Context
import android.content.SharedPreferences
import android.graphics.BitmapFactory
import android.net.Uri
import android.util.Log
import coil.Coil
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util.Locale
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * Dedicated, independent repository for Pocket bookmarks and saved web reading materials.
 * Decoupled completely from Downloader and MediaInbox functionality.
 * Supports automatic background fetching of link titles, descriptions, previews, and all site media.
 */
class PocketRepository(
    private val context: Context,
    private val coroutineScope: CoroutineScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
) {
    private val prefs: SharedPreferences =
        context.getSharedPreferences("pocket_bookmarks_prefs", Context.MODE_PRIVATE)

    private val pocketStorageDir: File = File(context.filesDir, "pocket").apply {
        if (!exists()) mkdirs()
    }
    private val bookmarksFile: File = File(pocketStorageDir, "bookmarks.json")
    private val bookmarkLock = Any()

    private val _bookmarks = MutableStateFlow<List<MediaInboxStack>>(emptyList())
    val bookmarks: StateFlow<List<MediaInboxStack>> = _bookmarks.asStateFlow()

    private val _showSvg = MutableStateFlow(prefs.getBoolean(KEY_SHOW_SVG, false))
    val showSvg: StateFlow<Boolean> = _showSvg.asStateFlow()

    private val _showVideos = MutableStateFlow(prefs.getBoolean(KEY_SHOW_VIDEOS, true))
    val showVideos: StateFlow<Boolean> = _showVideos.asStateFlow()

    private val _showStreams = MutableStateFlow(prefs.getBoolean(KEY_SHOW_STREAMS, true))
    val showStreams: StateFlow<Boolean> = _showStreams.asStateFlow()

    private val _showPng = MutableStateFlow(prefs.getBoolean(KEY_SHOW_PNG, true))
    val showPng: StateFlow<Boolean> = _showPng.asStateFlow()

    private val _showGif = MutableStateFlow(prefs.getBoolean(KEY_SHOW_GIF, true))
    val showGif: StateFlow<Boolean> = _showGif.asStateFlow()

    private val _minWidth = MutableStateFlow(
        prefs.getInt(KEY_MIN_WIDTH, DEFAULT_MIN_WIDTH).let {
            if (it <= 0 && !prefs.getBoolean("pocket_dim_filter_explicitly_set_zero", false)) DEFAULT_MIN_WIDTH else it
        }
    )
    val minWidth: StateFlow<Int> = _minWidth.asStateFlow()

    private val _minHeight = MutableStateFlow(
        prefs.getInt(KEY_MIN_HEIGHT, DEFAULT_MIN_HEIGHT).let {
            if (it <= 0 && !prefs.getBoolean("pocket_dim_filter_explicitly_set_zero", false)) DEFAULT_MIN_HEIGHT else it
        }
    )
    val minHeight: StateFlow<Int> = _minHeight.asStateFlow()

    private val _minSizeKb = MutableStateFlow(prefs.getInt(KEY_MIN_SIZE_KB, DEFAULT_MIN_SIZE_KB))
    val minSizeKb: StateFlow<Int> = _minSizeKb.asStateFlow()

    fun setShowSvg(enabled: Boolean) {
        _showSvg.value = enabled
        prefs.edit().putBoolean(KEY_SHOW_SVG, enabled).apply()
    }

    fun setShowVideos(enabled: Boolean) {
        _showVideos.value = enabled
        prefs.edit().putBoolean(KEY_SHOW_VIDEOS, enabled).apply()
    }

    fun setShowStreams(enabled: Boolean) {
        _showStreams.value = enabled
        prefs.edit().putBoolean(KEY_SHOW_STREAMS, enabled).apply()
    }

    fun setShowPng(enabled: Boolean) {
        _showPng.value = enabled
        prefs.edit().putBoolean(KEY_SHOW_PNG, enabled).apply()
    }

    fun setShowGif(enabled: Boolean) {
        _showGif.value = enabled
        prefs.edit().putBoolean(KEY_SHOW_GIF, enabled).apply()
    }

    fun setMinWidth(width: Int) {
        _minWidth.value = width
        prefs.edit()
            .putInt(KEY_MIN_WIDTH, width)
            .putBoolean("pocket_dim_filter_explicitly_set_zero", width == 0)
            .apply()
    }

    fun setMinHeight(height: Int) {
        _minHeight.value = height
        prefs.edit()
            .putInt(KEY_MIN_HEIGHT, height)
            .putBoolean("pocket_dim_filter_explicitly_set_zero", height == 0)
            .apply()
    }

    private val sizeCache = ConcurrentHashMap<String, Long>()
    private val activeProbes = ConcurrentHashMap.newKeySet<String>()
    private val sizeProbeSemaphore = Semaphore(8)

    fun setMinSizeKb(sizeKb: Int) {
        _minSizeKb.value = sizeKb
        prefs.edit().putInt(KEY_MIN_SIZE_KB, sizeKb).apply()
    }

    fun parseDimensionsFromText(text: String): Pair<Int?, Int?> {
        if (text.isBlank()) return null to null
        val match = Regex("""(?i)(\d{2,5})\s*([x*×_]|by)\s*(\d{2,5})""").find(text)
        if (match != null) {
            val w = match.groupValues[1].toIntOrNull()
            val h = match.groupValues[3].toIntOrNull()
            if (w != null && h != null && w > 0 && h > 0) {
                return w to h
            }
        }
        val wMatch = Regex("""(?i)[?&](?:w|width)=(\d{2,5})""").find(text)
        val hMatch = Regex("""(?i)[?&](?:h|height)=(\d{2,5})""").find(text)
        if (wMatch != null || hMatch != null) {
            val w = wMatch?.groupValues?.get(1)?.toIntOrNull()
            val h = hMatch?.groupValues?.get(1)?.toIntOrNull()
            return w to h
        }
        return null to null
    }

    fun parseMediaItemDimensions(item: MediaInboxItem): Pair<Int?, Int?> {
        var width = item.width.takeIf { it > 0 }
        var height = item.height.takeIf { it > 0 }

        // Only persisted/source-reported dimensions or decoded image bounds are authoritative.
        // Filename tokens and arbitrary URL parameters are not reliable pixel measurements.
        if (width != null && height != null) return width to height

        // Resolve real dimensions from a local image without decoding its full bitmap.
        fun decodeBounds(path: String): Pair<Int?, Int?> {
            val options = BitmapFactory.Options().apply { inJustDecodeBounds = true }
            return try {
                BitmapFactory.decodeFile(path, options)
                if (options.outWidth > 0 && options.outHeight > 0) {
                    options.outWidth to options.outHeight
                } else null to null
            } catch (_: Throwable) {
                null to null
            }
        }

        if (item.localUri.isNotBlank()) {
            try {
                val local = item.localUri
                val bounds = when {
                    local.startsWith("content://", ignoreCase = true) -> {
                        val options = BitmapFactory.Options().apply { inJustDecodeBounds = true }
                        context.contentResolver.openInputStream(Uri.parse(local))?.use { stream ->
                            BitmapFactory.decodeStream(stream, null, options)
                        }
                        if (options.outWidth > 0 && options.outHeight > 0) {
                            options.outWidth to options.outHeight
                        } else null to null
                    }
                    else -> {
                        val path = local.removePrefix("file://")
                        val file = File(path)
                        if (file.exists() && file.isFile) decodeBounds(file.absolutePath) else null to null
                    }
                }
                width = width ?: bounds.first
                height = height ?: bounds.second
                if (width != null && height != null) return width to height
            } catch (_: Throwable) {}
        }

        // Coil's disk cache contains the actual encoded image; inspect it only when already cached.
        if (item.sourceUrl.isNotBlank()) {
            try {
                val snapshot = Coil.imageLoader(context).diskCache?.openSnapshot(item.sourceUrl)
                if (snapshot != null) {
                    try {
                        val cached = snapshot.data.toFile()
                        if (cached.exists() && cached.length() > 0L) {
                            val bounds = decodeBounds(cached.absolutePath)
                            width = width ?: bounds.first
                            height = height ?: bounds.second
                        }
                    } finally {
                        snapshot.close()
                    }
                }
            } catch (_: Throwable) {}
        }

        return width to height
    }

    /**
     * Dimension filters are strict for images: every enabled threshold needs a known
     * measurement, and that measurement must meet the threshold. Video, audio and
     * stream items remain outside the image-dimension filter.
     */
    fun itemMatchesDimensions(item: MediaInboxItem, minWidth: Int, minHeight: Int): Boolean {
        if (minWidth <= 0 && minHeight <= 0) return true
        if (item.isVideo || item.isAudio || item.isStream) return true

        val (width, height) = parseMediaItemDimensions(item)
        val widthMatches = minWidth <= 0 || (width != null && width >= minWidth)
        val heightMatches = minHeight <= 0 || (height != null && height >= minHeight)
        return widthMatches && heightMatches
    }

    fun itemMatchesType(
        item: MediaInboxItem,
        showSvg: Boolean,
        showVideos: Boolean,
        showStreams: Boolean,
        showPng: Boolean,
        showGif: Boolean
    ): Boolean {
        if (!showSvg && MediaClassifier.isSvg(item.sourceUrl, format = item.format)) {
            return false
        }
        val isItemVideo = item.isVideo || item.format.equals("MP4", true) || item.format.equals("WEBM", true) || item.format.equals("MOV", true) || item.sourceUrl.endsWith(".mp4", true) || item.sourceUrl.endsWith(".webm", true) || item.sourceUrl.endsWith(".mov", true)
        if (!showVideos && isItemVideo) {
            return false
        }
        val isItemStream = item.isStream || item.format.equals("M3U8", true) || item.sourceUrl.contains(".m3u8", true) || item.sourceUrl.contains(".mpd", true)
        if (!showStreams && isItemStream) {
            return false
        }
        val isItemPng = item.format.equals("PNG", true) || item.sourceUrl.contains(".png", true) || item.title.contains(".png", true)
        if (!showPng && isItemPng) {
            return false
        }
        val isItemGif = item.format.equals("GIF", true) || item.sourceUrl.contains(".gif", true) || item.title.contains(".gif", true)
        if (!showGif && isItemGif) {
            return false
        }
        return true
    }

    /**
     * Returns only a known byte count: supplied file size, local file/provider size,
     * a cached encoded file size, an inline data-URI size, or a completed HEAD probe.
     * It deliberately does not estimate remote file sizes from dimensions, type or URL hints.
     * Unknown is returned as 0 and therefore fails any active minimum-size filter.
     */
    fun resolveItemSizeBytes(item: MediaInboxItem): Long {
        if (item.sizeBytes > 0L) {
            sizeCache[item.id] = item.sizeBytes
            if (item.sourceUrl.isNotBlank()) sizeCache[item.sourceUrl] = item.sizeBytes
            return item.sizeBytes
        }

        if (item.localUri.isNotBlank()) {
            val local = item.localUri
            if (local.startsWith("content://", ignoreCase = true)) {
                runCatching {
                    context.contentResolver.openFileDescriptor(Uri.parse(local), "r")?.use { pfd ->
                        if (pfd.statSize > 0L) {
                            sizeCache[item.id] = pfd.statSize
                            return pfd.statSize
                        }
                    }
                }
            } else {
                val path = local.removePrefix("file://")
                val file = File(path)
                if (file.exists() && file.isFile && file.length() > 0L) {
                    val length = file.length()
                    sizeCache[item.id] = length
                    if (item.sourceUrl.isNotBlank()) sizeCache[item.sourceUrl] = length
                    return length
                }
            }
        }

        val url = item.sourceUrl
        if (url.isBlank()) return 0L

        // HLS/DASH playlist length is not the total streamed media size.
        // Without an extractor-reported total or a completed local file, keep it unknown.
        if (item.isStream ||
            url.contains(".m3u8", ignoreCase = true) ||
            url.contains(".mpd", ignoreCase = true)
        ) return 0L

        sizeCache[url]?.takeIf { it > 0L }?.let { return it }
        sizeCache[item.id]?.takeIf { it > 0L }?.let { return it }

        if (url.startsWith("data:", ignoreCase = true)) {
            val payload = url.substringAfter(',', "")
            if (payload.isNotBlank()) {
                val estimated = if (url.contains(";base64", ignoreCase = true)) {
                    val padding = payload.takeLastWhile { it == '=' }.length.coerceAtMost(2)
                    (payload.length.toLong() * 3L) / 4L - padding
                } else {
                    runCatching { java.net.URLDecoder.decode(payload, "UTF-8").toByteArray(Charsets.UTF_8).size.toLong() }
                        .getOrDefault(0L)
                }
                if (estimated > 0L) {
                    sizeCache[url] = estimated
                    return estimated
                }
            }
        }

        try {
            val snapshot = Coil.imageLoader(context).diskCache?.openSnapshot(url)
            if (snapshot != null) {
                try {
                    val cachedFile = snapshot.data.toFile()
                    val length = if (cachedFile.exists()) cachedFile.length() else 0L
                    if (length > 0L) {
                        sizeCache[url] = length
                        sizeCache[item.id] = length
                        return length
                    }
                } finally {
                    snapshot.close()
                }
            }
        } catch (_: Throwable) {}

        triggerAsyncHeadSize(url, item.id)
        return 0L
    }

    /**
     * Evaluates a strict minimum-size rule. Unknown sizes do not pass.
     */
    fun itemMatchesSize(item: MediaInboxItem, minSizeKb: Int): Boolean {
        if (minSizeKb <= 0) return true
        val minSizeBytes = minSizeKb.toLong() * 1024L
        val actualSize = resolveItemSizeBytes(item)
        return actualSize > 0L && actualSize >= minSizeBytes
    }

    private fun triggerAsyncHeadSize(url: String, itemId: String) {
        if (!url.startsWith("http://", ignoreCase = true) && !url.startsWith("https://", ignoreCase = true)) return
        if (!activeProbes.add(url)) return
        coroutineScope.launch {
            try {
                sizeProbeSemaphore.withPermit {
                    val result = OkHttpDownloader.probeDirectMedia(url)
                    val contentLength = result.contentLength
                    if (result.isConfirmedDirectMedia && contentLength != null && contentLength > 0L) {
                        sizeCache[url] = contentLength
                        sizeCache[itemId] = contentLength
                        updateItemSizeInBookmarks(itemId, url, contentLength)
                    }
                }
            } catch (_: Exception) {
            } finally {
                activeProbes.remove(url)
            }
        }
    }

    private fun updateItemSizeInBookmarks(itemId: String, url: String, sizeBytes: Long) {
        synchronized(bookmarkLock) {
            var changed = false
            val latestList = _bookmarks.value.map { stack ->
                var stackChanged = false
                val updatedItems = stack.items.map { item ->
                    if ((item.id == itemId || item.sourceUrl == url) && item.sizeBytes != sizeBytes) {
                        stackChanged = true
                        item.copy(sizeBytes = sizeBytes)
                    } else item
                }
                if (stackChanged) {
                    changed = true
                    stack.copy(items = updatedItems)
                } else stack
            }
            if (changed) {
                _bookmarks.value = latestList
                persist()
            }
        }
    }

    /**
     * Sanitizes a bookmark stack according to showSvg, media types (Videos, Streams, PNGs, GIFs),
     * minimum dimensions (horizontal/vertical from 0 to infinity), and media size (0KB to 10MB).
     * Any media item failing the active filters is hidden from found media.
     * The bookmark stack itself is ALWAYS preserved.
     */
    fun sanitizeStack(
        stack: MediaInboxStack,
        showSvg: Boolean = _showSvg.value,
        minWidth: Int = _minWidth.value,
        minHeight: Int = _minHeight.value,
        showVideos: Boolean = _showVideos.value,
        showStreams: Boolean = _showStreams.value,
        showPng: Boolean = _showPng.value,
        showGif: Boolean = _showGif.value,
        minSizeKb: Int = _minSizeKb.value
    ): MediaInboxStack {
        val filteredItems = stack.items.filter { item ->
            itemMatchesType(item, showSvg, showVideos, showStreams, showPng, showGif) &&
                itemMatchesDimensions(item, minWidth, minHeight) &&
                itemMatchesSize(item, minSizeKb)
        }

        // previewImageUrl is page artwork (e.g. Open Graph cover), not a discovered
        // downloadable candidate. Keep it intact; the media filters apply only to items.
        return stack.copy(items = filteredItems)
    }

    /**
     * Backwards-compatible overload for sanitizeStackForSvg.
     */
    fun sanitizeStackForSvg(stack: MediaInboxStack, showSvg: Boolean): MediaInboxStack {
        return sanitizeStack(stack, showSvg = showSvg)
    }

    init {
        loadBookmarks()
    }

    private fun loadBookmarks() {
        var rawJson: String? = null
        if (bookmarksFile.exists() && bookmarksFile.length() > 0) {
            try {
                rawJson = bookmarksFile.readText()
            } catch (e: Exception) {
                Log.w(TAG, "Failed reading bookmarks from app-private file: ${e.message}")
            }
        }
        if (rawJson.isNullOrBlank()) {
            val prefsJson = prefs.getString(KEY_BOOKMARKS_JSON, null)
            if (!prefsJson.isNullOrBlank()) {
                rawJson = prefsJson
            }
        }
        if (rawJson.isNullOrBlank()) {
            migrateFromLegacyInboxIfNeeded()
            return
        }

        try {
            val jsonArray = JSONArray(rawJson)
            val list = mutableListOf<MediaInboxStack>()
            for (i in 0 until jsonArray.length()) {
                val obj = jsonArray.getJSONObject(i)
                val itemsArr = obj.optJSONArray("items")
                val itemsList = mutableListOf<MediaInboxItem>()
                if (itemsArr != null) {
                    for (j in 0 until itemsArr.length()) {
                        val itmObj = itemsArr.getJSONObject(j)
                        itemsList.add(
                            MediaInboxItem(
                                id = itmObj.getString("id"),
                                title = itmObj.optString("title", "Media"),
                                localUri = itmObj.optString("localUri", ""),
                                sourceUrl = itmObj.optString("sourceUrl", ""),
                                isVideo = itmObj.optBoolean("isVideo", false),
                                isAudio = itmObj.optBoolean("isAudio", false),
                                isStream = itmObj.optBoolean("isStream", false),
                                format = itmObj.optString("format", ""),
                                sizeBytes = itmObj.optLong("sizeBytes", 0L),
                                dateAddedMillis = itmObj.optLong("dateAddedMillis", System.currentTimeMillis()),
                                width = itmObj.optInt("width", 0),
                                height = itmObj.optInt("height", 0)
                            )
                        )
                    }
                }

                list.add(
                    MediaInboxStack(
                        id = obj.getString("id"),
                        title = obj.optString("title", "Untitled Bookmark"),
                        sourceDomain = obj.optString("sourceDomain", ""),
                        sourceUrl = obj.optString("sourceUrl", ""),
                        items = itemsList,
                        timestampMillis = obj.optLong("timestampMillis", System.currentTimeMillis()),
                        description = obj.optString("description", ""),
                        previewImageUrl = obj.optString("previewImageUrl", "")
                    )
                )
            }
            _bookmarks.value = list

            // If loaded from SharedPreferences migration, persist to persistent file and clean up prefs
            if (!bookmarksFile.exists() || bookmarksFile.length() == 0L) {
                persist()
                prefs.edit().remove(KEY_BOOKMARKS_JSON).apply()
            }
        } catch (e: Exception) {
            Log.e(TAG, "Error loading bookmarks", e)
            _bookmarks.value = emptyList()
        }
    }

    private fun migrateFromLegacyInboxIfNeeded() {
        try {
            val legacyPrefs = context.getSharedPreferences("media_inbox_prefs", Context.MODE_PRIVATE)
            val legacyJson = legacyPrefs.getString("inbox_stacks_json", null)
            if (!legacyJson.isNullOrBlank()) {
                val jsonArray = JSONArray(legacyJson)
                val list = mutableListOf<MediaInboxStack>()
                for (i in 0 until jsonArray.length()) {
                    val obj = jsonArray.getJSONObject(i)
                    val id = obj.getString("id")
                    val itemsArr = obj.optJSONArray("items")
                    val isBookmark = id.startsWith("bookmark_") || itemsArr == null || itemsArr.length() == 0
                    if (isBookmark) {
                        list.add(
                            MediaInboxStack(
                                id = id,
                                title = obj.optString("title", "Untitled Bookmark"),
                                sourceDomain = obj.optString("sourceDomain", ""),
                                sourceUrl = obj.optString("sourceUrl", ""),
                                items = emptyList(),
                                timestampMillis = obj.optLong("timestampMillis", System.currentTimeMillis()),
                                description = obj.optString("description", ""),
                                previewImageUrl = obj.optString("previewImageUrl", "")
                            )
                        )
                    }
                }
                if (list.isNotEmpty()) {
                    _bookmarks.value = list
                    persist()
                }
            }
        } catch (_: Exception) {}
    }

    private fun persist() {
        synchronized(bookmarkLock) {
            try {
                val snapshot = _bookmarks.value
                val jsonArray = JSONArray()
                snapshot.forEach { bookmark ->
                    val obj = JSONObject().apply {
                        put("id", bookmark.id)
                        put("title", bookmark.title)
                        put("sourceDomain", bookmark.sourceDomain)
                        put("sourceUrl", bookmark.sourceUrl)
                        put("timestampMillis", bookmark.timestampMillis)
                        put("description", bookmark.description)
                        put("previewImageUrl", bookmark.previewImageUrl)

                        val itemsArr = JSONArray()
                        bookmark.items.forEach { itm ->
                            val itmObj = JSONObject().apply {
                                put("id", itm.id)
                                put("title", itm.title)
                                put("localUri", itm.localUri)
                                put("sourceUrl", itm.sourceUrl)
                                put("isVideo", itm.isVideo)
                                put("isAudio", itm.isAudio)
                                put("isStream", itm.isStream)
                                put("format", itm.format)
                                put("sizeBytes", itm.sizeBytes)
                                put("dateAddedMillis", itm.dateAddedMillis)
                                put("width", itm.width)
                                put("height", itm.height)
                            }
                            itemsArr.put(itmObj)
                        }
                        put("items", itemsArr)
                    }
                    jsonArray.put(obj)
                }

                if (!pocketStorageDir.exists() && !pocketStorageDir.mkdirs()) {
                    throw IllegalStateException("Could not create Pocket storage directory")
                }

                val tmpFile = File(pocketStorageDir, "bookmarks.json.tmp")
                tmpFile.writeText(jsonArray.toString())
                try {
                    Files.move(
                        tmpFile.toPath(),
                        bookmarksFile.toPath(),
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING
                    )
                } catch (_: java.nio.file.AtomicMoveNotSupportedException) {
                    Files.move(
                        tmpFile.toPath(),
                        bookmarksFile.toPath(),
                        StandardCopyOption.REPLACE_EXISTING
                    )
                }
            } catch (e: Exception) {
                Log.e(TAG, "Error persisting bookmarks to app-private storage", e)
            }
        }
    }
    /**
     * Adds a bookmark to Pocket.
     * When autoFetchMetadata is true, automatically queries the web page in the background
     * for its title, description, link preview image, and site media items, then smoothly updates the bookmark.
     */
    fun addBookmark(
        title: String,
        url: String,
        domain: String = "",
        description: String = "",
        previewImageUrl: String = "",
        autoFetchMetadata: Boolean = true,
        initialItems: List<MediaInboxItem> = emptyList()
    ): MediaInboxStack {
        val cleanUrl = url.trim()
        val cleanDomain = domain.ifBlank { MediaClassifier.cleanDomainFromUrl(cleanUrl) }

        val cleanLower = cleanUrl.lowercase(Locale.ROOT).substringBefore('?').substringBefore('#')
        val ext = cleanLower.substringAfterLast('.', "")
        val isDirectImage = ext in listOf("jpg", "jpeg", "png", "webp", "gif", "avif", "bmp", "svg", "heic", "heif", "ico") ||
                cleanUrl.lowercase(Locale.ROOT).let { lower ->
                    lower.contains("format=jpg") || lower.contains("format=png") ||
                    lower.contains("format=webp") || lower.contains("format=avif") ||
                    lower.startsWith("data:image/")
                }

        val isDirectVideo = ext in listOf("mp4", "webm", "mov", "mkv", "m3u8", "mpd", "avi", "3gp", "ts") ||
                cleanUrl.lowercase(Locale.ROOT).let { lower ->
                    lower.contains("format=mp4") || lower.contains("format=webm") ||
                    lower.startsWith("data:video/")
                }

        val isMedia = isDirectImage || isDirectVideo

        val mediaFileName = cleanUrl.substringAfterLast('/').substringBefore('?').ifBlank {
            cleanDomain.ifBlank { "Media" }
        }

        val directItems = if (isMedia) {
            listOf(
                MediaInboxItem(
                    id = "media_${System.currentTimeMillis()}_${cleanUrl.hashCode().toUInt().toString(16)}",
                    title = title.ifBlank { mediaFileName },
                    localUri = if (cleanUrl.startsWith("file://") || cleanUrl.startsWith("content://")) cleanUrl else "",
                    sourceUrl = cleanUrl,
                    isVideo = isDirectVideo,
                    dateAddedMillis = System.currentTimeMillis()
                )
            )
        } else {
            emptyList()
        }

        val bookmarkKey = MediaClassifier.bookmarkDeduplicationKey(cleanUrl)
        val existingBookmark = if (bookmarkKey.isBlank()) {
            null
        } else {
            _bookmarks.value.firstOrNull { existing ->
                existing.sourceUrl.isNotBlank() &&
                    MediaClassifier.bookmarkDeduplicationKey(existing.sourceUrl) == bookmarkKey
            }
        }
        val rawItems = directItems + initialItems + (existingBookmark?.items ?: emptyList())
        val finalItems = MediaNormalizer.deduplicateMediaInboxItems(rawItems)

        val fallbackTitle = if (isMedia) mediaFileName else cleanDomain.ifBlank { cleanUrl }
        val incomingTitle = title.ifBlank { fallbackTitle }
        val incomingTitleIsGeneric = title.isBlank() ||
            title.equals(cleanDomain, ignoreCase = true) ||
            title.equals(cleanUrl, ignoreCase = true) ||
            title == "Untitled Bookmark"
        val existingTitleIsMeaningful = existingBookmark != null &&
            existingBookmark.title.isNotBlank() &&
            !existingBookmark.title.equals(existingBookmark.sourceDomain, ignoreCase = true) &&
            !existingBookmark.title.equals(existingBookmark.sourceUrl, ignoreCase = true) &&
            existingBookmark.title != "Untitled Bookmark"
        val resolvedTitle = if (existingTitleIsMeaningful && incomingTitleIsGeneric) {
            existingBookmark!!.title
        } else {
            incomingTitle
        }

        // Preserve the first saved URL and enriched fields when another tracking-decorated
        // version of the same page is saved later.
        val initialPreview = previewImageUrl.ifBlank {
            existingBookmark?.previewImageUrl?.takeIf { it.isNotBlank() }
                ?: if (isMedia) cleanUrl
                else finalItems.firstOrNull { !it.isVideo && !it.isStream && it.sourceUrl.isNotBlank() }?.sourceUrl
                    ?: finalItems.firstOrNull { it.sourceUrl.isNotBlank() }?.sourceUrl
                    ?: ""
        }

        val bookmark = MediaInboxStack(
            id = existingBookmark?.id ?: "bookmark_${UUID.randomUUID()}",
            title = resolvedTitle,
            sourceDomain = existingBookmark?.sourceDomain?.ifBlank { cleanDomain } ?: cleanDomain,
            sourceUrl = existingBookmark?.sourceUrl ?: cleanUrl,
            items = finalItems,
            timestampMillis = System.currentTimeMillis(),
            description = description.ifBlank {
                existingBookmark?.description?.takeIf { it.isNotBlank() }
                    ?: if (isMedia) (if (isDirectVideo) "Saved Video" else "Saved Image") else ""
            },
            previewImageUrl = initialPreview
        )
        val updated = listOf(bookmark) + _bookmarks.value.filter { existing ->
            val isCanonicalDuplicate = bookmarkKey.isNotBlank() &&
                existing.sourceUrl.isNotBlank() &&
                MediaClassifier.bookmarkDeduplicationKey(existing.sourceUrl) == bookmarkKey
            existing.id != bookmark.id && !isCanonicalDuplicate
        }
        synchronized(bookmarkLock) {
            // Merge with the latest list at commit time in case another bookmark was saved
            // while metadata or a scan callback was updating Pocket.
            val latest = _bookmarks.value
            val committed = listOf(bookmark) + latest.filter { existing ->
                existing.id != bookmark.id &&
                    (bookmarkKey.isBlank() ||
                        existing.sourceUrl.isBlank() ||
                        MediaClassifier.bookmarkDeduplicationKey(existing.sourceUrl) != bookmarkKey)
            }
            _bookmarks.value = committed
            persist()
        }

        if (autoFetchMetadata && cleanUrl.isNotBlank()) {
            fetchAndApplyMetadata(bookmark.id, cleanUrl)
            if (!isMedia) {
                startDeepScan(bookmark.id, context)
            }
        }

        return bookmark
    }

    /**
     * Automatically fetches metadata (title, description, and site media) for a bookmark and updates it.
     */
    fun fetchAndApplyMetadata(bookmarkId: String, url: String = "") {
        if (!activeExtractions.add(bookmarkId)) return
        coroutineScope.launch {
            try {
                val current = _bookmarks.value.find { it.id == bookmarkId } ?: return@launch
                val targetUrl = url.ifBlank { current.sourceUrl }
                if (targetUrl.isBlank()) return@launch

                val meta = LinkMetadataFetcher.fetch(targetUrl)
                val latest = _bookmarks.value.find { it.id == bookmarkId } ?: return@launch

                val cleanDomain = latest.sourceDomain.ifBlank { MediaClassifier.cleanDomainFromUrl(targetUrl) }
                val isTitleDefault = latest.title.isBlank() ||
                        latest.title.equals(cleanDomain, ignoreCase = true) ||
                        latest.title.equals(targetUrl, ignoreCase = true) ||
                        latest.title == "Untitled Bookmark"

                val newTitle = if (isTitleDefault && meta.title.isNotBlank()) meta.title else latest.title
                val newDesc = if (latest.description.isBlank() && meta.description.isNotBlank()) meta.description else latest.description
                val newDomain = if (latest.sourceDomain.isBlank() && meta.domain.isNotBlank()) meta.domain else latest.sourceDomain

                val metadataItems = meta.mediaItems.filterNot { item ->
                    item.sourceUrl.contains("google.com/s2/favicons", ignoreCase = true)
                }
                val retainedItems = latest.items.filterNot { item ->
                    item.sourceUrl.contains("google.com/s2/favicons", ignoreCase = true)
                }
                val newItems = if (retainedItems.isEmpty() && metadataItems.isNotEmpty()) {
                    metadataItems
                } else if (retainedItems.isNotEmpty()) {
                    if (metadataItems.isNotEmpty()) {
                        MediaNormalizer.deduplicateMediaInboxItems(retainedItems + metadataItems)
                    } else {
                        retainedItems
                    }
                } else {
                    emptyList()
                }

                val hasRealCurrentPreview = latest.previewImageUrl.isNotBlank() &&
                    !latest.previewImageUrl.contains("google.com/s2/favicons", ignoreCase = true)
                val metadataPreview = meta.previewImageUrl.takeIf {
                    it.isNotBlank() && !it.contains("google.com/s2/favicons", ignoreCase = true)
                }
                val newPreview = when {
                    hasRealCurrentPreview -> latest.previewImageUrl
                    metadataPreview != null -> metadataPreview
                    else -> newItems.firstOrNull { !it.isVideo && !it.isStream && it.sourceUrl.isNotBlank() }?.sourceUrl
                        ?: newItems.firstOrNull { it.sourceUrl.isNotBlank() }?.sourceUrl
                        ?: latest.previewImageUrl.takeIf { !it.contains("google.com/s2/favicons", ignoreCase = true) }.orEmpty()
                }

                if (newTitle != latest.title || newDesc != latest.description || newPreview != latest.previewImageUrl ||
                    newDomain != latest.sourceDomain || newItems != latest.items) {
                    val updated = latest.copy(
                        title = newTitle,
                        description = newDesc,
                        previewImageUrl = newPreview,
                        sourceDomain = newDomain,
                        items = newItems
                    )
                    updateBookmark(updated)
                }
            } catch (e: Exception) {
                Log.e(TAG, "Error fetching metadata for bookmark $bookmarkId", e)
            } finally {
                activeExtractions.remove(bookmarkId)
            }
        }
    }

    private val activeExtractions = ConcurrentHashMap.newKeySet<String>()

    fun isExtracting(bookmarkId: String): Boolean = activeExtractions.contains(bookmarkId)

    /**
     * Specifically extracts media and refreshes metadata for a bookmark only upon explicit user request.
     * Guaranteed to be cache-first, memory-friendly (zero hidden WebViews), concurrency-guarded,
     * and preserves existing cached media if refresh fails or yields empty results.
     */
    fun fetchSiteMedia(bookmarkId: String, extractionContext: Context? = null) {
        if (!activeExtractions.add(bookmarkId)) {
            Log.d(TAG, "Extraction already running for bookmark $bookmarkId, skipping duplicate request")
            return
        }
        coroutineScope.launch {
            try {
                val latest = _bookmarks.value.find { it.id == bookmarkId } ?: return@launch
                if (latest.sourceUrl.isBlank()) return@launch

                // Lightweight HTTP + Jsoup / yt-dlp extraction with zero hidden WebViews to prevent RAM spikes
                val meta = try {
                    LinkMetadataFetcher.fetch(latest.sourceUrl)
                } catch (e: Exception) {
                    Log.w(TAG, "Extraction failed for bookmark $bookmarkId: ${e.message}. Preserving cached media.")
                    null
                }

                val current = _bookmarks.value.find { it.id == bookmarkId } ?: return@launch
                if (meta == null) {
                    // Refresh failed: preserve existing cached media and metadata intact!
                    return@launch
                }

                // Filter out external favicon urls from media items
                val newCandidates = meta.mediaItems.filterNot { item ->
                    item.sourceUrl.contains("google.com/s2/favicons", ignoreCase = true)
                }

                // Resolve media items: merge new candidates with existing cached items
                // Groups CDN size variants, selects the largest variant, and prioritizes video streams
                val retainedItems = current.items.filterNot { item ->
                    item.sourceUrl.contains("google.com/s2/favicons", ignoreCase = true)
                }
                val resolvedItems = if (newCandidates.isNotEmpty()) {
                    val combined = newCandidates + retainedItems
                    MediaNormalizer.deduplicateMediaInboxItems(combined)
                } else {
                    // Preserve existing real media while removing generic site-icon pseudo-items.
                    retainedItems
                }

                val metadataPreview = meta.previewImageUrl.takeIf {
                    it.isNotBlank() && !it.contains("google.com/s2/favicons", ignoreCase = true)
                }
                val currentPreview = current.previewImageUrl.takeIf {
                    it.isNotBlank() && !it.contains("google.com/s2/favicons", ignoreCase = true)
                }
                val newPreview = metadataPreview
                    ?: resolvedItems.firstOrNull { !it.isVideo && !it.isStream && it.sourceUrl.isNotBlank() }?.sourceUrl
                    ?: resolvedItems.firstOrNull { it.sourceUrl.isNotBlank() }?.sourceUrl
                    ?: currentPreview
                    ?: ""

                val cleanDomain = current.sourceDomain.ifBlank { meta.domain }
                val isTitleDefault = current.title.isBlank() ||
                    current.title.equals(cleanDomain, ignoreCase = true) ||
                    current.title.equals(current.sourceUrl, ignoreCase = true) ||
                    current.title == "Untitled Bookmark"

                val updated = current.copy(
                    title = if (isTitleDefault && meta.title.isNotBlank()) meta.title else current.title,
                    description = if (current.description.isBlank() && meta.description.isNotBlank()) meta.description else current.description,
                    previewImageUrl = newPreview,
                    sourceDomain = cleanDomain,
                    items = resolvedItems
                )
                updateBookmark(updated)
                Log.d(TAG, "Bookmark media refresh completed: ${resolvedItems.size} items for $bookmarkId")
            } catch (e: Exception) {
                Log.e(TAG, "Error in fetchSiteMedia for bookmark $bookmarkId, preserving cached media", e)
            } finally {
                activeExtractions.remove(bookmarkId)
            }
        }
    }

    /**
     * Refreshes metadata and media for a bookmark only upon explicit user request.
     */
    fun refreshMetadata(bookmarkId: String, extractionContext: Context? = null) {
        fetchSiteMedia(bookmarkId, extractionContext)
    }

    private val _activeDeepScanId = MutableStateFlow<String?>(null)
    @Volatile private var activeDeepScanToken: String? = null
    val activeDeepScanId: StateFlow<String?> = _activeDeepScanId.asStateFlow()

    fun isDeepScanning(bookmarkId: String): Boolean = _activeDeepScanId.value == bookmarkId

    /**
     * Starts a browser-backed deep scan for a bookmark.
     * Continues until the page is exhausted, memory protection stops it, or the user stops it.
     * Newly discovered media is merged and checkpointed incrementally.
     */
    fun startDeepScan(bookmarkId: String, context: Context = this.context) {
        val target = _bookmarks.value.find { it.id == bookmarkId } ?: return
        if (target.sourceUrl.isBlank()) return

        // A generation token prevents an older scan's completion callback from clearing
        // the visible state of a newer scan, including a restart of the same bookmark.
        val scanToken = UUID.randomUUID().toString()
        activeDeepScanToken = scanToken
        _activeDeepScanId.value = bookmarkId

        PocketDeepScanEngine.startScan(
            context = context,
            bookmarkId = bookmarkId,
            targetUrl = target.sourceUrl,
            scope = coroutineScope,
            onIncrementalMediaDiscovered = { newCandidates ->
                synchronized(bookmarkLock) {
                    if (activeDeepScanToken != scanToken) return@synchronized
                    val current = _bookmarks.value.find { it.id == bookmarkId } ?: return@synchronized
                    val combined = newCandidates + current.items
                    val resolvedItems = MediaNormalizer.deduplicateMediaInboxItems(combined)
                    val newPreview = if (current.previewImageUrl.isBlank()) {
                        resolvedItems.firstOrNull { !it.isVideo }?.sourceUrl
                            ?: resolvedItems.firstOrNull()?.sourceUrl
                            ?: ""
                    } else current.previewImageUrl

                    val updated = current.copy(
                        items = resolvedItems,
                        previewImageUrl = newPreview
                    )
                    _bookmarks.value = _bookmarks.value.map { if (it.id == bookmarkId) updated else it }
                    // Checkpoint each discovered batch so process death cannot erase
                    // media that has already appeared in Pocket.
                    persist()
                }
            },
            onCompleted = {
                synchronized(bookmarkLock) {
                    if (activeDeepScanToken == scanToken) {
                        activeDeepScanToken = null
                        if (_activeDeepScanId.value == bookmarkId) {
                            _activeDeepScanId.value = null
                        }
                    }
                    persist()
                }
            }
        )
    }

    /**
     * Stops the active scan for this bookmark and flushes the latest findings.
     */
    fun stopDeepScan(bookmarkId: String) {
        if (_activeDeepScanId.value == bookmarkId) {
            activeDeepScanToken = null
            PocketDeepScanEngine.stopScan()
            _activeDeepScanId.value = null
            persist()
        }
    }

    /**
     * Intentionally no-op on startup to enforce cache-first and zero network overhead on startup.
     */
    fun autoFetchMissingMetadata() {
        // Intentionally no-op on startup to ensure zero network requests and maximum responsiveness
    }

    fun updateBookmark(updated: MediaInboxStack) {
        synchronized(bookmarkLock) {
            val latest = _bookmarks.value.firstOrNull { it.id == updated.id }
            // Do not let a metadata refresh or note edit overwrite media discovered or
            // downloaded by another operation while the UI was holding an older snapshot.
            val mergedItems = if (latest == null) updated.items else {
                MediaNormalizer.deduplicateMediaInboxItems(updated.items + latest.items)
            }
            _bookmarks.value = _bookmarks.value.map {
                if (it.id == updated.id) updated.copy(items = mergedItems) else it
            }
            persist()
        }
    }

    /**
     * One canonical media download can satisfy the same asset in multiple bookmarks.
     * Update every matching reference rather than relying on one origin stack ID.
     */
    fun updateDownloadedMedia(mediaUrl: String, itemId: String, localUri: String, sizeBytes: Long) {
        val canonicalKey = MediaClassifier.deduplicationKey(mediaUrl)
        synchronized(bookmarkLock) {
            var changed = false
            _bookmarks.value = _bookmarks.value.map { stack ->
                var stackChanged = false
                val items = stack.items.map { item ->
                    val sameUrl = canonicalKey.isNotBlank() &&
                        MediaClassifier.deduplicationKey(item.sourceUrl) == canonicalKey
                    if ((item.id == itemId || sameUrl) &&
                        (item.localUri != localUri || item.sizeBytes != sizeBytes)
                    ) {
                        stackChanged = true
                        item.copy(localUri = localUri, sizeBytes = sizeBytes)
                    } else item
                }
                val previewMatches = stack.previewImageUrl.isNotBlank() &&
                    canonicalKey.isNotBlank() &&
                    MediaClassifier.deduplicationKey(stack.previewImageUrl) == canonicalKey
                if (stackChanged || previewMatches) {
                    changed = true
                    stack.copy(items = items, previewImageUrl = if (previewMatches) localUri else stack.previewImageUrl)
                } else stack
            }
            if (changed) persist()
        }
    }

    fun deleteBookmark(bookmarkId: String) {
        if (_activeDeepScanId.value == bookmarkId) stopDeepScan(bookmarkId)
        synchronized(bookmarkLock) {
            _bookmarks.value = _bookmarks.value.filter { it.id != bookmarkId }
            persist()
        }
    }

    fun deleteBookmarks(bookmarkIds: Set<String>) {
        _activeDeepScanId.value?.takeIf { it in bookmarkIds }?.let(::stopDeepScan)
        synchronized(bookmarkLock) {
            _bookmarks.value = _bookmarks.value.filter { it.id !in bookmarkIds }
            persist()
        }
    }

    fun clearAll() {
        _activeDeepScanId.value?.let(::stopDeepScan)
        synchronized(bookmarkLock) {
            activeDeepScanToken = null
            _bookmarks.value = emptyList()
            prefs.edit().remove(KEY_BOOKMARKS_JSON).apply()
            runCatching { bookmarksFile.delete() }
        }
    }

    companion object {
        private const val TAG = "PocketRepository"
        private const val KEY_BOOKMARKS_JSON = "pocket_bookmarks_json"
        private const val KEY_SHOW_SVG = "pocket_filter_show_svg"
        private const val KEY_SHOW_VIDEOS = "pocket_filter_show_videos"
        private const val KEY_SHOW_STREAMS = "pocket_filter_show_streams"
        private const val KEY_SHOW_PNG = "pocket_filter_show_png"
        private const val KEY_SHOW_GIF = "pocket_filter_show_gif"
        private const val KEY_MIN_WIDTH = "pocket_filter_min_width"
        private const val KEY_MIN_HEIGHT = "pocket_filter_min_height"
        private const val KEY_MIN_SIZE_KB = "pocket_filter_min_size_kb"
        const val DEFAULT_MIN_WIDTH = 480
        const val DEFAULT_MIN_HEIGHT = 540
        const val DEFAULT_MIN_SIZE_KB = 10

        @Volatile
        private var INSTANCE: PocketRepository? = null

        fun getInstance(context: Context): PocketRepository {
            return INSTANCE ?: synchronized(this) {
                INSTANCE ?: PocketRepository(context.applicationContext).also { INSTANCE = it }
            }
        }
    }
}
