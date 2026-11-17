package com.amresalehin.emregallery

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import com.amresalehin.emregallery.data.LinkMetadataFetcher
import com.amresalehin.emregallery.data.MediaInboxItem
import com.amresalehin.emregallery.data.MediaInboxStack
import com.amresalehin.emregallery.data.PocketRepository
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.runBlocking
import java.io.File
import org.jsoup.Jsoup
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config
class LinkMetadataUnitTest {

    private lateinit var context: Context

    @Before
    fun setUp() {
        context = ApplicationProvider.getApplicationContext()
        context.getSharedPreferences("pocket_bookmarks_prefs", Context.MODE_PRIVATE)
            .edit()
            .clear()
            .commit()
        File(context.filesDir, "pocket/bookmarks.json").delete()
    }

    @Test
    fun testParseOpenGraphMetadata() {
        val html = """
            <!DOCTYPE html>
            <html>
            <head>
                <meta property="og:title" content="Jetpack Compose Architecture Guide &amp; Best Practices" />
                <meta property="og:description" content="A comprehensive deep dive into modern Android UI development with Jetpack Compose." />
                <meta property="og:image" content="https://developer.android.com/images/compose-hero.png" />
                <meta property="og:site_name" content="Android Developers" />
                <link rel="icon" href="/favicon.ico" />
            </head>
            <body>
                <h1>Android Compose</h1>
            </body>
            </html>
        """.trimIndent()

        val metadata = LinkMetadataFetcher.parseHtml(html, "https://developer.android.com/jetpack/compose")

        assertEquals("Jetpack Compose Architecture Guide & Best Practices", metadata.title)
        assertEquals("A comprehensive deep dive into modern Android UI development with Jetpack Compose.", metadata.description)
        assertEquals("https://developer.android.com/images/compose-hero.png", metadata.previewImageUrl)
        assertEquals("Android Developers", metadata.siteName)
        assertEquals("developer.android.com", metadata.domain)
        assertEquals("https://developer.android.com/favicon.ico", metadata.faviconUrl)
    }

    @Test
    fun testParseTwitterCardMetadataFallback() {
        val html = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Fallback Standard Title</title>
                <meta name="twitter:title" content="Twitter Card Title Example" />
                <meta name="twitter:description" content="Twitter Card Summary Description" />
                <meta name="twitter:image" content="/assets/twitter-card.webp" />
            </head>
            <body>
            </body>
            </html>
        """.trimIndent()

        val metadata = LinkMetadataFetcher.parseHtml(html, "https://news.example.org/article/42")

        assertEquals("Twitter Card Title Example", metadata.title)
        assertEquals("Twitter Card Summary Description", metadata.description)
        assertEquals("https://news.example.org/assets/twitter-card.webp", metadata.previewImageUrl)
        assertEquals("news.example.org", metadata.domain)
    }

    @Test
    fun testParseStandardHtmlMetadataFallback() {
        val html = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Standard Page Title - Example Blog</title>
                <meta name="description" content="This is a standard meta description tag for search engines." />
                <link rel="image_src" href="//cdn.example.org/preview.jpg" />
                <link rel="apple-touch-icon" href="https://example.org/apple-touch-icon.png" />
            </head>
            <body>
                <p>Some body text that shouldn't override meta description.</p>
            </body>
            </html>
        """.trimIndent()

        val metadata = LinkMetadataFetcher.parseHtml(html, "https://example.org/post/100")

        assertEquals("Standard Page Title - Example Blog", metadata.title)
        assertEquals("This is a standard meta description tag for search engines.", metadata.description)
        assertEquals("https://cdn.example.org/preview.jpg", metadata.previewImageUrl)
    }

    @Test
    fun testRelativeImageResolution() {
        val html = """
            <!DOCTYPE html>
            <html>
            <head>
                <meta property="og:title" content="Documentation Page" />
                <meta property="og:image" content="images/art.jpg" />
            </head>
            <body></body>
            </html>
        """.trimIndent()

        val metadata = LinkMetadataFetcher.parseHtml(html, "https://docs.kotlinlang.org/intro/start.html")

        assertEquals("https://docs.kotlinlang.org/intro/images/art.jpg", metadata.previewImageUrl)
    }

    @Test
    fun testPocketRepositoryAutomaticMetadataAndPersistence() = runBlocking {
        val testScope = CoroutineScope(Dispatchers.Unconfined + SupervisorJob())
        val repo = PocketRepository(context, testScope)

        // Add a bookmark with title, description, and previewImageUrl
        val bookmark = repo.addBookmark(
            title = "Sample Title",
            url = "https://example.com/article",
            description = "Sample Description",
            previewImageUrl = "https://example.com/cover.jpg",
            autoFetchMetadata = false
        )

        assertEquals("Sample Title", bookmark.title)
        assertEquals("Sample Description", bookmark.description)
        assertEquals("https://example.com/cover.jpg", bookmark.previewImageUrl)
        assertEquals(1, repo.bookmarks.value.size)

        // Verify persistence in SharedPreferences
        val repo2 = PocketRepository(context, testScope)
        val restored = repo2.bookmarks.value.first()
        assertEquals(bookmark.id, restored.id)
        assertEquals("Sample Title", restored.title)
        assertEquals("Sample Description", restored.description)
        assertEquals("https://example.com/cover.jpg", restored.previewImageUrl)
    }

    @Test
    fun testMediaInboxStackPreviewImageUrlField() {
        val stackWithPreview = MediaInboxStack(
            id = "test_preview_1",
            title = "Article Preview",
            sourceDomain = "blog.example.com",
            sourceUrl = "https://blog.example.com/post",
            items = emptyList(),
            description = "Post description",
            previewImageUrl = "https://blog.example.com/hero.jpg"
        )

        assertEquals("https://blog.example.com/hero.jpg", stackWithPreview.previewImageUrl)
        assertTrue(stackWithPreview.items.isEmpty())

        val stackWithoutPreview = MediaInboxStack(
            id = "test_preview_2",
            title = "No Preview",
            sourceDomain = "example.com",
            sourceUrl = "https://example.com",
            items = emptyList()
        )

        assertEquals("", stackWithoutPreview.previewImageUrl)
    }

    @Test
    fun testParseJsonLdMetadata() {
        val html = """
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <script type="application/ld+json">
                {
                    "@context": "https://schema.org",
                    "@graph": [
                        {
                            "@type": "WebPage",
                            "name": "Magnific AI (by Freepik) | The AI Creative Platform",
                            "description": "The complete platform of creative AI tools for image, video, and audio generation.",
                            "image": "https://media.magnific.com/meta-tags-social/mg-home-og.jpg?w=1200&amp;h=675"
                        }
                    ]
                }
                </script>
            </head>
            <body></body>
            </html>
        """.trimIndent()

        val metadata = LinkMetadataFetcher.parseHtml(html, "https://magnific.com")

        assertEquals("Magnific AI (by Freepik) | The AI Creative Platform", metadata.title)
        assertEquals("The complete platform of creative AI tools for image, video, and audio generation.", metadata.description)
        assertEquals("https://media.magnific.com/meta-tags-social/mg-home-og.jpg?w=1200&h=675", metadata.previewImageUrl)
        assertFalse(metadata.mediaItems.isEmpty())
        assertEquals("https://media.magnific.com/meta-tags-social/mg-home-og.jpg?w=1200&h=675", metadata.mediaItems.first().sourceUrl)
    }

    @Test
    fun testHtmlEntityUnescapingInImageUrls() {
        val html = """
            <!DOCTYPE html>
            <html>
            <head>
                <meta property="og:title" content="Entity Test" />
                <meta property="og:image" content="https://cdn.example.org/photo.jpg?size=large&amp;quality=high" />
            </head>
            <body></body>
            </html>
        """.trimIndent()

        val metadata = LinkMetadataFetcher.parseHtml(html, "https://example.org/item")
        assertEquals("https://cdn.example.org/photo.jpg?size=large&quality=high", metadata.previewImageUrl)
    }

    @Test
    fun testGalleryDlCheckSupportPopularSites() {
        val ytSupport = com.amresalehin.emregallery.data.GalleryDlEngine.checkSupport("https://www.youtube.com/watch?v=dQw4w9WgXcQ")
        assertTrue("YouTube should be recognized as supported extractor", ytSupport.isSupported)

        val redditSupport = com.amresalehin.emregallery.data.GalleryDlEngine.checkSupport("https://reddit.com/r/EarthPorn/comments/abc123")
        assertTrue("Reddit should be recognized as supported extractor", redditSupport.isSupported)

        val xSupport = com.amresalehin.emregallery.data.GalleryDlEngine.checkSupport("https://x.com/Android/status/123456789")
        assertTrue("X / Twitter should be recognized as supported extractor", xSupport.isSupported)

        val tiktokSupport = com.amresalehin.emregallery.data.GalleryDlEngine.checkSupport("https://www.tiktok.com/@user/video/123456")
        assertTrue("TikTok should be recognized as supported extractor", tiktokSupport.isSupported)
    }

    @Test
    fun testAddBookmarkTreatsTrackedAndReorderedUrlsAsDuplicate() {
        val testScope = CoroutineScope(Dispatchers.Unconfined + SupervisorJob())
        val repository = PocketRepository(context, testScope)

        val first = repository.addBookmark(
            title = "A useful article",
            url = "https://EXAMPLE.com:443/articles/./economics?id=42&lang=en&utm_source=newsletter&fbclid=tracker",
            description = "The original description",
            previewImageUrl = "https://example.com/cover.jpg",
            autoFetchMetadata = false
        )

        val duplicate = repository.addBookmark(
            title = "example.com",
            url = "https://example.com/articles/economics?lang=en&id=42&gclid=tracking",
            autoFetchMetadata = false
        )

        assertEquals("A tracked/reordered URL must reuse the existing bookmark ID", first.id, duplicate.id)
        assertEquals("Only one bookmark should remain", 1, repository.bookmarks.value.size)
        assertEquals("Keep the first saved source URL", first.sourceUrl, duplicate.sourceUrl)
        assertEquals("Do not replace an enriched title with a generic domain title", "A useful article", duplicate.title)
        assertEquals("Keep the existing description", "The original description", duplicate.description)
        assertEquals("Keep the existing preview", "https://example.com/cover.jpg", duplicate.previewImageUrl)
    }

    @Test
    fun testPocketDimensionFiltersRejectUnknownAndBelowThresholdMeasurements() {
        val scope = CoroutineScope(Dispatchers.Unconfined + SupervisorJob())
        val repository = PocketRepository(context, scope)

        val exactMinimum = MediaInboxItem(
            id = "exact",
            title = "wallpaper.jpg",
            localUri = "",
            sourceUrl = "",
            isVideo = false,
            width = 480,
            height = 540,
            sizeBytes = 10L * 1024L
        )
        val tooSmallWidth = exactMinimum.copy(id = "small-width", width = 479)
        val missingHeight = exactMinimum.copy(id = "missing-height", height = 0)
        val unknownDimensions = MediaInboxItem(
            id = "unknown-dimensions",
            title = "wallpaper_4000x3000.jpg",
            localUri = "",
            sourceUrl = "https://cdn.example.com/wallpaper_4000x3000.jpg?w=4000&h=3000",
            isVideo = false
        )
        val videoWithoutDimensions = MediaInboxItem(
            id = "video",
            title = "clip.mp4",
            localUri = "",
            sourceUrl = "",
            isVideo = true,
            format = "MP4"
        )

        assertTrue(repository.itemMatchesDimensions(exactMinimum, 480, 540))
        assertFalse(repository.itemMatchesDimensions(tooSmallWidth, 480, 540))
        assertFalse(repository.itemMatchesDimensions(missingHeight, 480, 540))
        assertFalse(repository.itemMatchesDimensions(unknownDimensions, 480, 540))
        assertTrue("Image dimension filters do not apply to video/audio/stream entries", repository.itemMatchesDimensions(videoWithoutDimensions, 480, 540))
        assertTrue("Zero thresholds mean any dimensions", repository.itemMatchesDimensions(unknownDimensions, 0, 0))
    }

    @Test
    fun testPocketMinimumSizeUsesKnownBytesAndRejectsEstimates() {
        val scope = CoroutineScope(Dispatchers.Unconfined + SupervisorJob())
        val repository = PocketRepository(context, scope)

        val exactMinimum = MediaInboxItem(
            id = "exact-size",
            title = "image.jpg",
            localUri = "",
            sourceUrl = "",
            isVideo = false,
            sizeBytes = 10L * 1024L
        )
        val oneByteBelow = exactMinimum.copy(id = "below-size", sizeBytes = 10L * 1024L - 1L)
        val unknownSize = MediaInboxItem(
            id = "unknown-size",
            title = "large_4000x3000.jpg",
            localUri = "",
            sourceUrl = "opaque://example.com/image.jpg?size=90000000",
            isVideo = false,
            sizeBytes = 0L
        )

        assertTrue(repository.itemMatchesSize(exactMinimum, 10))
        assertFalse(repository.itemMatchesSize(oneByteBelow, 10))
        assertEquals("Unknown size must remain unknown rather than receive an estimate", 0L, repository.resolveItemSizeBytes(unknownSize))
        assertFalse("Estimated or URL-hint sizes must not pass a strict minimum", repository.itemMatchesSize(unknownSize, 10))
        assertTrue(repository.itemMatchesSize(unknownSize, 0))
    }

    @Test
    fun testPocketSizeFilterDoesNotUseHlsManifestLengthAsMediaSize() {
        val scope = CoroutineScope(Dispatchers.Unconfined + SupervisorJob())
        val repository = PocketRepository(context, scope)
        val manifest = MediaInboxItem(
            id = "hls-manifest",
            title = "live.m3u8",
            localUri = "",
            sourceUrl = "https://cdn.example.com/live.m3u8?size=90000000",
            isVideo = true,
            isStream = true,
            format = "M3U8"
        )

        assertEquals(0L, repository.resolveItemSizeBytes(manifest))
        assertFalse(repository.itemMatchesSize(manifest, 10))
    }

    @Test
    fun testSanitizeStackFiltersUnknownCandidatesButPreservesPagePreview() {
        val scope = CoroutineScope(Dispatchers.Unconfined + SupervisorJob())
        val repository = PocketRepository(context, scope)
        val stack = MediaInboxStack(
            id = "preview-filter-test",
            title = "Article with preview",
            sourceDomain = "example.com",
            sourceUrl = "https://example.com/article",
            items = listOf(
                MediaInboxItem(
                    id = "known",
                    title = "cover.jpg",
                    localUri = "",
                    sourceUrl = "",
                    isVideo = false,
                    width = 1200,
                    height = 800,
                    sizeBytes = 200L * 1024L
                ),
                MediaInboxItem(
                    id = "unknown",
                    title = "unmeasured.jpg",
                    localUri = "",
                    sourceUrl = "",
                    isVideo = false
                )
            ),
            previewImageUrl = "https://example.com/og-cover.jpg"
        )

        val sanitized = repository.sanitizeStack(
            stack = stack,
            minWidth = 480,
            minHeight = 540,
            minSizeKb = 10
        )

        assertEquals(listOf("known"), sanitized.items.map { it.id })
        assertEquals(
            "The page preview is separate from downloadable candidates and must remain available",
            "https://example.com/og-cover.jpg",
            sanitized.previewImageUrl
        )
    }

    @Test
    fun testPocketItemDimensionsAndSizeSurvivePersistence() {
        val scope = CoroutineScope(Dispatchers.Unconfined + SupervisorJob())
        val repository = PocketRepository(context, scope)
        val saved = repository.addBookmark(
            title = "Measured image",
            url = "https://example.com/measured",
            autoFetchMetadata = false,
            initialItems = listOf(
                MediaInboxItem(
                    id = "measured",
                    title = "photo.jpg",
                    localUri = "",
                    sourceUrl = "https://cdn.example.com/photo.jpg",
                    isVideo = false,
                    sizeBytes = 42_000L,
                    width = 1600,
                    height = 1200
                )
            )
        )

        val restoredRepository = PocketRepository(context, scope)
        val restored = restoredRepository.bookmarks.value.first { it.id == saved.id }.items.single()
        assertEquals(1600, restored.width)
        assertEquals(1200, restored.height)
        assertEquals(42_000L, restored.sizeBytes)
    }

    @Test
    fun testStaticHtmlExtractionDoesNotTreatDisplayAttributesAsIntrinsicDimensions() {
        val doc = Jsoup.parse(
            """
                <html><body>
                    <article>
                        <img src="/images/hero.jpg" alt="Hero" width="1280" height="720">
                    </article>
                </body></html>
            """.trimIndent(),
            "https://example.com/article"
        )

        val items = LinkMetadataFetcher.extractSiteMedia(
            doc = doc,
            baseUrl = "https://example.com/article",
            defaultTitle = "Article"
        )
        val hero = items.first { it.sourceUrl == "https://example.com/images/hero.jpg" }

        assertEquals("HTML display width is not proof of intrinsic dimensions", 0, hero.width)
        assertEquals("HTML display height is not proof of intrinsic dimensions", 0, hero.height)
    }

    @Test
    fun testPocketSizeResolverUsesLocalFileBytesWithoutEstimating() {
        val scope = CoroutineScope(Dispatchers.Unconfined + SupervisorJob())
        val repository = PocketRepository(context, scope)
        val file = File(context.cacheDir, "pocket-size-check.bin")
        file.writeBytes(ByteArray(12 * 1024))

        val item = MediaInboxItem(
            id = "local-size-check",
            title = "local.bin",
            localUri = file.absolutePath,
            sourceUrl = "",
            isVideo = false
        )

        assertEquals(file.length(), repository.resolveItemSizeBytes(item))
        assertTrue(repository.itemMatchesSize(item, 10))
        file.delete()
    }
}
