package com.amresalehin.emregallery.data

import android.content.Context
import androidx.work.Constraints
import androidx.work.Data
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.WorkInfo
import androidx.work.WorkManager
import kotlinx.coroutines.flow.Flow
import org.json.JSONObject
import java.security.MessageDigest
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * Persistent media download queue.
 *
 * Work identity is derived from a SHA-256 digest of the canonical media URL. Multiple
 * requesting items can reference one WorkManager job, but they are registered as aliases
 * so progress/completion can be reconciled without creating a second worker.
 */
object DownloadQueueManager {

    private val networkConstraints = Constraints.Builder()
        .setRequiredNetworkType(NetworkType.CONNECTED)
        .build()

    private val _inMemoryDownloading = kotlinx.coroutines.flow.MutableStateFlow(false)
    val inMemoryDownloading: kotlinx.coroutines.flow.StateFlow<Boolean> = _inMemoryDownloading

    private val activeDownloadCount = AtomicInteger(0)
    private val activeItemIds = ConcurrentHashMap.newKeySet<String>()
    private val activeWorkIds = ConcurrentHashMap.newKeySet<String>()
    private val itemWorkIds = ConcurrentHashMap<String, MutableSet<String>>()
    private val workItemsById = ConcurrentHashMap<String, MutableSet<String>>()
    private val uniqueWorkLocks = Array(64) { Any() }

    fun isDownloading(itemId: String): Boolean {
        if (activeItemIds.contains(itemId)) return true
        val activity = DownloadActivityManager.activities.value.firstOrNull { it.id == itemId }
        return activity?.status == DownloadActivityStatus.DOWNLOADING ||
            activity?.status == DownloadActivityStatus.RETRYING
    }

    /**
     * A collision-resistant, deterministic WorkManager key. Keep the full digest: a 32-bit
     * String.hashCode() can collide and abs(Int.MIN_VALUE) remains negative.
     */
    internal fun uniqueWorkNameForUrl(url: String): String {
        val canonical = MediaClassifier.deduplicationKey(url).ifBlank { url.trim() }
        val digest = MessageDigest.getInstance("SHA-256")
            .digest(canonical.toByteArray(Charsets.UTF_8))
            .joinToString("") { byte -> "%02x".format(byte) }
        return "download_$digest"
    }

    private fun isActive(state: WorkInfo.State): Boolean {
        return state == WorkInfo.State.ENQUEUED ||
            state == WorkInfo.State.RUNNING ||
            state == WorkInfo.State.BLOCKED
    }

    private fun registerActiveWork(workId: UUID, itemId: String) {
        val workKey = workId.toString()
        workItemsById.computeIfAbsent(workKey) { ConcurrentHashMap.newKeySet() }.add(itemId)
        itemWorkIds.computeIfAbsent(itemId) { ConcurrentHashMap.newKeySet() }.add(workKey)
        activeItemIds.add(itemId)

        // Count a WorkManager job once even when several item IDs alias that same job.
        if (activeWorkIds.add(workKey)) {
            activeDownloadCount.incrementAndGet()
            _inMemoryDownloading.value = true
        }
    }

    private fun unregisterActiveWork(workId: UUID) {
        val workKey = workId.toString()
        val itemIds = workItemsById.remove(workKey).orEmpty()
        itemIds.forEach { itemId ->
            itemWorkIds[itemId]?.let { ids ->
                ids.remove(workKey)
                if (ids.isEmpty()) {
                    itemWorkIds.remove(itemId, ids)
                    activeItemIds.remove(itemId)
                }
            }
        }

        if (activeWorkIds.remove(workKey)) {
            val remaining = activeDownloadCount.decrementAndGet().coerceAtLeast(0)
            activeDownloadCount.set(remaining)
            _inMemoryDownloading.value = remaining > 0
        }
    }

    /**
     * Legacy start hook retained for callers/tests that do not have a WorkManager ID.
     * New enqueue paths register against a concrete WorkManager ID instead.
     */
    fun markDownloadStarted(count: Int = 1, itemId: String? = null) {
        if (itemId != null) activeItemIds.add(itemId)
        if (count > 0) {
            activeDownloadCount.addAndGet(count)
            _inMemoryDownloading.value = true
        }
    }

    fun markDownloadCompleted(itemId: String? = null, workId: UUID? = null) {
        val resolvedWorkId = workId ?: itemId?.let { id ->
            itemWorkIds[id]?.firstOrNull()?.let { runCatching { UUID.fromString(it) }.getOrNull() }
        }
        if (resolvedWorkId != null) {
            unregisterActiveWork(resolvedWorkId)
            return
        }

        if (itemId != null) {
            // Preserve the legacy itemId counter API, but decrement only when this ID is
            // still registered. A late callback after cancellation is therefore idempotent.
            if (activeItemIds.remove(itemId)) {
                val remaining = activeDownloadCount.decrementAndGet().coerceAtLeast(0)
                activeDownloadCount.set(remaining)
                _inMemoryDownloading.value = remaining > 0
            }
            return
        }

        // Compatibility path for work started through the legacy counter API.
        val remaining = activeDownloadCount.decrementAndGet().coerceAtLeast(0)
        activeDownloadCount.set(remaining)
        _inMemoryDownloading.value = remaining > 0
    }

    private fun serializeHeaders(headers: Map<String, String>): String? {
        if (headers.isEmpty()) return null
        return runCatching {
            val json = JSONObject()
            headers.forEach { (k, v) ->
                // Guard against exceeding WorkManager's input Data size limit.
                if (v.length <= 2048) json.put(k, v)
            }
            json.toString().takeIf { it.length <= 4096 }
        }.getOrNull()
    }

    private fun buildRequest(
        item: ScrapedMediaItem,
        referer: String?,
        stackId: String,
        stackTitle: String,
        sourceDomain: String,
        sourceUrl: String
    ): androidx.work.OneTimeWorkRequest {
        val headersJson = serializeHeaders(item.downloadHeaders)
        val inputData = Data.Builder()
            .putString(MediaDownloadWorker.KEY_MEDIA_URL, item.url)
            .putString(MediaDownloadWorker.KEY_FILE_NAME, item.title)
            .putString(MediaDownloadWorker.KEY_REFERER, referer)
            .putString(MediaDownloadWorker.KEY_ITEM_ID, item.id)
            .putString(MediaDownloadWorker.KEY_TITLE, item.title)
            .putBoolean(MediaDownloadWorker.KEY_IS_VIDEO, item.isVideo)
            .putBoolean(MediaDownloadWorker.KEY_IS_AUDIO, item.isAudio)
            .putBoolean(MediaDownloadWorker.KEY_IS_STREAM, item.isStream)
            .putString(MediaDownloadWorker.KEY_FORMAT, item.format)
            .putString(MediaDownloadWorker.KEY_STACK_ID, stackId)
            .putString(MediaDownloadWorker.KEY_STACK_TITLE, stackTitle)
            .putString(MediaDownloadWorker.KEY_SOURCE_DOMAIN, sourceDomain)
            .putString(MediaDownloadWorker.KEY_SOURCE_URL, sourceUrl)
            .putString(MediaDownloadWorker.KEY_SOURCE_ENGINE, item.provenance.extractorEngine)
            .apply {
                if (!headersJson.isNullOrBlank()) {
                    putString(MediaDownloadWorker.KEY_HEADERS_JSON, headersJson)
                }
            }
            .build()

        return OneTimeWorkRequestBuilder<MediaDownloadWorker>()
            .setConstraints(networkConstraints)
            .setInputData(inputData)
            .addTag(MediaDownloadWorker.WORK_TAG_DOWNLOAD)
            .addTag("item_${item.id}")
            .build()
    }

    fun enqueueDownload(
        context: Context,
        item: ScrapedMediaItem,
        referer: String? = null,
        stackId: String? = null,
        stackTitle: String? = null,
        sourceDomain: String? = null,
        sourceUrl: String? = null,
        isRetry: Boolean = false
    ): UUID {
        val sId = stackId ?: "stack_${System.currentTimeMillis()}"
        val sTitle = stackTitle ?: sourceDomain ?: "Discovered Media"
        val domain = sourceDomain ?: ""
        val url = sourceUrl ?: item.url

        DownloadActivityManager.initialize(context)
        val workManager = WorkManager.getInstance(context)
        val uniqueWorkName = uniqueWorkNameForUrl(item.url)
        val lock = uniqueWorkLocks[(uniqueWorkName.hashCode() and Int.MAX_VALUE) % uniqueWorkLocks.size]

        return synchronized(lock) {
            val activeBefore = runCatching {
                workManager.getWorkInfosForUniqueWork(uniqueWorkName).get()
            }.getOrDefault(emptyList()).firstOrNull { isActive(it.state) }

            if (activeBefore != null) {
                // This request aliases the existing file download. Do not create a second
                // activity row: the existing WorkManager worker owns progress and completion.
                registerActiveWork(activeBefore.id, item.id)

                // Work may complete between the unique-work query and alias registration.
                // Re-check its individual ID so a late alias cannot leave a phantom active count.
                val confirmedState = runCatching {
                    workManager.getWorkInfoById(activeBefore.id).get()?.state
                }.getOrNull()
                if (confirmedState == null || isActive(confirmedState)) {
                    return@synchronized activeBefore.id
                }

                // The worker completed between the first lookup and this confirmation.
                // Remove its aliases and fall through to enqueue a fresh request.
                unregisterActiveWork(activeBefore.id)
            }

            val request = buildRequest(item, referer, sId, sTitle, domain, url)

            // Record the owning activity before enqueueing so a fast worker cannot finish
            // before its activity is created.
            DownloadActivityManager.recordQueued(
                id = item.id,
                title = item.title,
                mediaUrl = item.url,
                thumbnailUrl = item.thumbnailUrl ?: item.url,
                isVideo = item.isVideo,
                isAudio = item.isAudio,
                isStream = item.isStream,
                format = item.format,
                workId = request.id.toString(),
                isRetry = isRetry,
                stackId = sId,
                stackTitle = sTitle,
                sourceDomain = domain,
                sourceUrl = url
            )
            registerActiveWork(request.id, item.id)

            try {
                // KEEP never cancels an unfinished worker. A completed/failed unique-work
                // chain is eligible for a new request, so REPLACE is not needed for retry.
                workManager.enqueueUniqueWork(uniqueWorkName, ExistingWorkPolicy.KEEP, request)
                    .result.get()

                val actualWork = runCatching {
                    workManager.getWorkInfosForUniqueWork(uniqueWorkName).get()
                }.getOrDefault(emptyList()).firstOrNull { isActive(it.state) }

                if (actualWork != null && actualWork.id != request.id) {
                    // Another process/request won the WorkManager race. Keep the canonical
                    // worker as owner and remove the transient activity row for this request.
                    unregisterActiveWork(request.id)
                    registerActiveWork(actualWork.id, item.id)
                    DownloadActivityManager.deleteActivity(item.id)
                    actualWork.id
                } else {
                    if (actualWork == null) unregisterActiveWork(request.id)
                    actualWork?.id ?: request.id
                }
            } catch (e: Exception) {
                unregisterActiveWork(request.id)
                DownloadActivityManager.markFailed(item.id, e.message ?: "Could not enqueue download")
                throw e
            }
        }
    }

    fun enqueueBatch(
        context: Context,
        items: List<ScrapedMediaItem>,
        referer: String? = null,
        stackId: String? = null,
        stackTitle: String? = null,
        sourceDomain: String? = null,
        sourceUrl: String? = null
    ): List<UUID> {
        // Use the same unique-work, alias-registration and retry semantics as single downloads.
        val distinctItems = MediaClassifier.deduplicateScrapedItems(items)
        return distinctItems.map { item ->
            enqueueDownload(
                context = context,
                item = item,
                referer = referer,
                stackId = stackId,
                stackTitle = stackTitle,
                sourceDomain = sourceDomain,
                sourceUrl = sourceUrl
            )
        }
    }

    fun getActiveDownloadsFlow(context: Context): Flow<List<WorkInfo>> {
        return WorkManager.getInstance(context)
            .getWorkInfosByTagFlow(MediaDownloadWorker.WORK_TAG_DOWNLOAD)
    }

    fun cancelDownload(context: Context, workId: UUID) {
        runCatching {
            WorkManager.getInstance(context).cancelWorkById(workId)
        }
        // Cancellation is idempotent; the worker's finally block may call this again.
        unregisterActiveWork(workId)
    }

    fun cancelAll(context: Context) {
        runCatching {
            WorkManager.getInstance(context).cancelAllWorkByTag(MediaDownloadWorker.WORK_TAG_DOWNLOAD)
        }
        activeItemIds.clear()
        activeWorkIds.clear()
        itemWorkIds.clear()
        workItemsById.clear()
        activeDownloadCount.set(0)
        _inMemoryDownloading.value = false
    }
}
