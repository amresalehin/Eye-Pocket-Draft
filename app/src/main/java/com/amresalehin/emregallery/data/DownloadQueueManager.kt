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
import java.util.UUID
import kotlin.math.abs

/**
 * Manages persistent media downloads through Android Jetpack WorkManager.
 * Schedules, tracks, and cancels background downloads with network constraints.
 * Keys unique work by deduped URL and synchronizes active work IDs with running jobs.
 */
object DownloadQueueManager {

    private val networkConstraints = Constraints.Builder()
        .setRequiredNetworkType(NetworkType.CONNECTED)
        .build()

    private val _inMemoryDownloading = kotlinx.coroutines.flow.MutableStateFlow(false)
    val inMemoryDownloading: kotlinx.coroutines.flow.StateFlow<Boolean> = _inMemoryDownloading

    private val activeDownloadCount = java.util.concurrent.atomic.AtomicInteger(0)
    private val activeItemIds = java.util.concurrent.ConcurrentHashMap.newKeySet<String>()

    /**
     * YouTube playback URLs are format-specific (itag). Key them by the originating
     * video page so changing quality/format does not enqueue a second download.
     */
    private fun stableQueueIdentity(item: ScrapedMediaItem, fallbackPageUrl: String): String {
        val pageUrl = item.metadata["source_url"]?.takeIf { it.isNotBlank() } ?: fallbackPageUrl
        val videoId = MediaClassifier.extractYouTubeVideoId(pageUrl)
            ?: MediaClassifier.extractYouTubeVideoId(item.url)
        return videoId?.let { "youtube:$it" } ?: item.url
    }

    fun isDownloading(itemId: String): Boolean {
        if (activeItemIds.contains(itemId)) return true
        val activity = DownloadActivityManager.activities.value.firstOrNull { it.id == itemId }
        return activity?.status == DownloadActivityStatus.DOWNLOADING || activity?.status == DownloadActivityStatus.RETRYING
    }

    fun markDownloadStarted(count: Int = 1, itemId: String? = null) {
        // Two callers may race before WorkManager persists the first unique request.
        // Track and increment only once for a given worker item ID.
        if (itemId != null && !activeItemIds.add(itemId)) return
        activeDownloadCount.addAndGet(count)
        _inMemoryDownloading.value = true
    }

    fun markDownloadCompleted(itemId: String? = null) {
        // Worker completion and explicit cancellation can race. Only the first completion
        // for a tracked item decrements the active count.
        if (itemId != null && !activeItemIds.remove(itemId)) return
        val remaining = activeDownloadCount.decrementAndGet()
        if (remaining <= 0) {
            activeDownloadCount.set(0)
            _inMemoryDownloading.value = false
        }
    }

    private fun serializeHeaders(headers: Map<String, String>): String? {
        if (headers.isEmpty()) return null
        return runCatching {
            val json = JSONObject()
            headers.forEach { (k, v) ->
                // Guard against exceeding the 10KB WorkManager Data limit by capping individual header values
                if (v.length <= 2048) {
                    json.put(k, v)
                }
            }
            json.toString().takeIf { it.length <= 4096 }
        }.getOrNull()
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
            .putBoolean(
                MediaDownloadWorker.KEY_REQUIRES_AUDIO,
                item.metadata["requires_audio"].equals("true", ignoreCase = true)
            )
            .putString(MediaDownloadWorker.KEY_FORMAT, item.format)
            .putString(MediaDownloadWorker.KEY_STACK_ID, sId)
            .putString(MediaDownloadWorker.KEY_STACK_TITLE, sTitle)
            .putString(MediaDownloadWorker.KEY_SOURCE_DOMAIN, domain)
            .putString(MediaDownloadWorker.KEY_SOURCE_URL, url)
            .putString(MediaDownloadWorker.KEY_SOURCE_ENGINE, item.provenance.extractorEngine)
            .apply {
                if (!headersJson.isNullOrBlank()) {
                    putString(MediaDownloadWorker.KEY_HEADERS_JSON, headersJson)
                }
            }
            .build()

        val request = OneTimeWorkRequestBuilder<MediaDownloadWorker>()
            .setConstraints(networkConstraints)
            .setInputData(inputData)
            .addTag(MediaDownloadWorker.WORK_TAG_DOWNLOAD)
            .addTag("item_${item.id}")
            .build()

        val dedupKey = MediaClassifier.deduplicationKey(item.url)
        val queueIdentity = stableQueueIdentity(item, url)
        val uniqueWorkName = "download_${MediaNormalizer.stableIdentityHash(queueIdentity)}"
        // Existing installations may still have active work under the old Int-hash name.
        val legacyUniqueWorkName = "download_${abs(dedupKey.hashCode())}"

        val workManager = WorkManager.getInstance(context)
        val existingWorkList = runCatching {
            (workManager.getWorkInfosForUniqueWork(uniqueWorkName).get() +
                workManager.getWorkInfosForUniqueWork(legacyUniqueWorkName).get()).distinctBy { it.id }
        }.getOrDefault(emptyList())

        val activeWork = existingWorkList.firstOrNull {
            it.state == WorkInfo.State.ENQUEUED || it.state == WorkInfo.State.RUNNING
        }

        val finalWorkId: UUID
        val shouldRecordActivity: Boolean
        if (activeWork != null) {
            // An existing worker can only complete its own item ID. Do not create a
            // second queue record that will remain DOWNLOADING forever.
            finalWorkId = activeWork.id
            shouldRecordActivity = activeWork.inputData.getString(MediaDownloadWorker.KEY_ITEM_ID) == item.id
        } else {
            finalWorkId = request.id
            workManager.enqueueUniqueWork(
                uniqueWorkName,
                if (isRetry) ExistingWorkPolicy.REPLACE else ExistingWorkPolicy.KEEP,
                request
            )
            markDownloadStarted(count = 1, itemId = item.id)
            shouldRecordActivity = true
        }

        DownloadActivityManager.initialize(context)
        if (shouldRecordActivity) {
            DownloadActivityManager.recordQueued(
                id = item.id,
                title = item.title,
                mediaUrl = item.url,
                thumbnailUrl = item.thumbnailUrl ?: item.url,
                isVideo = item.isVideo,
                isAudio = item.isAudio,
                isStream = item.isStream,
                format = item.format,
                workId = finalWorkId.toString(),
                isRetry = isRetry,
                stackId = sId,
                stackTitle = sTitle,
                sourceDomain = domain,
                sourceUrl = url
            )
        }

        return finalWorkId
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
        val sId = stackId ?: "stack_${System.currentTimeMillis()}"
        val sTitle = stackTitle ?: sourceDomain ?: "Discovered Media"
        val domain = sourceDomain ?: ""
        val url = sourceUrl ?: referer ?: ""

        val workManager = WorkManager.getInstance(context)
        val distinctItems = MediaClassifier.deduplicateScrapedItems(items)

        val activityItems = mutableListOf<ScrapedMediaItem>()
        val activityWorkIds = mutableListOf<UUID>()
        val workIds = distinctItems.map { item ->
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
                .putBoolean(
                    MediaDownloadWorker.KEY_REQUIRES_AUDIO,
                    item.metadata["requires_audio"].equals("true", ignoreCase = true)
                )
                .putString(MediaDownloadWorker.KEY_FORMAT, item.format)
                .putString(MediaDownloadWorker.KEY_STACK_ID, sId)
                .putString(MediaDownloadWorker.KEY_STACK_TITLE, sTitle)
                .putString(MediaDownloadWorker.KEY_SOURCE_DOMAIN, domain)
                .putString(MediaDownloadWorker.KEY_SOURCE_URL, url)
                .putString(MediaDownloadWorker.KEY_SOURCE_ENGINE, item.provenance.extractorEngine)
                .apply {
                    if (!headersJson.isNullOrBlank()) {
                        putString(MediaDownloadWorker.KEY_HEADERS_JSON, headersJson)
                    }
                }
                .build()

            val request = OneTimeWorkRequestBuilder<MediaDownloadWorker>()
                .setConstraints(networkConstraints)
                .setInputData(inputData)
                .addTag(MediaDownloadWorker.WORK_TAG_DOWNLOAD)
                .addTag("item_${item.id}")
                .build()

            val dedupKey = MediaClassifier.deduplicationKey(item.url)
            val queueIdentity = stableQueueIdentity(item, url)
            val uniqueWorkName = "download_${MediaNormalizer.stableIdentityHash(queueIdentity)}"
            val legacyUniqueWorkName = "download_${abs(dedupKey.hashCode())}"

            val existingWorkList = runCatching {
                (workManager.getWorkInfosForUniqueWork(uniqueWorkName).get() +
                    workManager.getWorkInfosForUniqueWork(legacyUniqueWorkName).get()).distinctBy { it.id }
            }.getOrDefault(emptyList())

            val activeWork = existingWorkList.firstOrNull {
                it.state == WorkInfo.State.ENQUEUED || it.state == WorkInfo.State.RUNNING
            }

            if (activeWork != null) {
                // Never attach an untracked alias ID to an existing worker.
                if (activeWork.inputData.getString(MediaDownloadWorker.KEY_ITEM_ID) == item.id) {
                    activityItems.add(item)
                    activityWorkIds.add(activeWork.id)
                }
                activeWork.id
            } else {
                workManager.enqueueUniqueWork(
                    uniqueWorkName,
                    ExistingWorkPolicy.KEEP,
                    request
                )
                markDownloadStarted(count = 1, itemId = item.id)
                activityItems.add(item)
                activityWorkIds.add(request.id)
                request.id
            }
        }

        DownloadActivityManager.initialize(context)
        if (activityItems.isNotEmpty()) {
            DownloadActivityManager.recordBatchQueued(
                items = activityItems,
                workIds = activityWorkIds,
                stackId = sId,
                stackTitle = sTitle,
                sourceDomain = domain,
                sourceUrl = url
            )
        }

        return workIds
    }

    fun getActiveDownloadsFlow(context: Context): Flow<List<WorkInfo>> {
        return WorkManager.getInstance(context)
            .getWorkInfosByTagFlow(MediaDownloadWorker.WORK_TAG_DOWNLOAD)
    }

    fun cancelDownload(context: Context, workId: UUID) {
        val workManager = WorkManager.getInstance(context)
        val itemId = runCatching {
            workManager.getWorkInfoById(workId).get()?.inputData
                ?.getString(MediaDownloadWorker.KEY_ITEM_ID)
        }.getOrNull()
        runCatching { workManager.cancelWorkById(workId) }
        // Handles ENQUEUED work cancelled before doWork/finally can run; worker completion
        // may race this call, but item-ID completion is idempotent.
        if (!itemId.isNullOrBlank()) markDownloadCompleted(itemId)
    }

    fun cancelAll(context: Context) {
        runCatching {
            WorkManager.getInstance(context).cancelAllWorkByTag(MediaDownloadWorker.WORK_TAG_DOWNLOAD)
        }
        activeItemIds.clear()
        activeDownloadCount.set(0)
        _inMemoryDownloading.value = false
    }
}
