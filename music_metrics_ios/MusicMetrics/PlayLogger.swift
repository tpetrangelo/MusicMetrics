import Foundation
import MusicKit
import CoreLocation

@MainActor
class PlayLogger: NSObject, ObservableObject {
    static let shared = PlayLogger()

    private let player = SystemMusicPlayer.shared
    private let location = LocationManager.shared
    private let sender = EventSender.shared

    @Published var recentEvents: [PlayEvent] = []
    @Published var isLoggingEnabled: Bool = true

    private var pollTimer: Timer?
    private var commitTimer: Timer?
    private let iso8601 = ISO8601DateFormatter()

    // Track currently being evaluated
    private var pendingTrackID: String?
    private var pendingTrackStartedAt: Date?
    private var pendingSong: Song?

    // Pause/resume tracking
    private var requiredListenSeconds: Double = 0   // 75% of track duration
    private var accumulatedSeconds: Double = 0       // how much we've actually listened
    private var playbackStartedAt: Date?             // when current play segment started
    private var isTrackPaused: Bool = false

    // Track that has already been committed — removed, duplicates are intentional

    private override init() {
        super.init()
        recentEvents = LocalStore.load()
    }

    // MARK: - Lifecycle

    func start() async {
        location.start()

        pollTimer = Timer.scheduledTimer(withTimeInterval: 3.0, repeats: true) { [weak self] _ in
            Task { @MainActor [weak self] in
                self?.checkCurrentTrack()
            }
        }
    }

    // MARK: - Track Detection

    private func checkCurrentTrack() {
        let status = player.state.playbackStatus
        let isPlaying = status == .playing
        let isStopped = status == .stopped || status == .paused

        guard let entry = player.queue.currentEntry,
              let item = entry.item else {
            // Queue is empty — full reset
            resetPendingState()
            return
        }

        if case .song(let song) = item {
            let id = song.id.rawValue

            // Playback stopped/paused with no track change —
            // this catches the "song ended" scenario where queue
            // still holds the last track but nothing is playing
            if isStopped && !isTrackPaused && id == pendingTrackID {
                isTrackPaused = true
                commitTimer?.invalidate()
                commitTimer = nil
                if let start = playbackStartedAt {
                    accumulatedSeconds += Date().timeIntervalSince(start)
                    playbackStartedAt = nil
                }
                print("[PlayLogger] ⏸ stopped/paused — accumulated \(Int(accumulatedSeconds))s")
                return
            }

            // New track detected — reset everything completely
            if id != pendingTrackID {
                resetPendingState()

                pendingTrackID = id
                pendingSong = song
                pendingTrackStartedAt = Date()

                if isPlaying {
                    playbackStartedAt = Date()
                    scheduleCommit(for: song, remainingSeconds: song.duration.map { $0 * 0.75 } ?? 0)
                } else {
                    isTrackPaused = true
                }
                return
            }

            // Same track — handle resume only
            if isPlaying && isTrackPaused {
                isTrackPaused = false
                playbackStartedAt = Date()
                let remaining = max(requiredListenSeconds - accumulatedSeconds, 0)
                print("[PlayLogger] ▶️ resumed — \(Int(remaining))s remaining to commit")
                if let song = pendingSong {
                    scheduleCommit(for: song, remainingSeconds: remaining)
                }
            }
        }
    }

    // MARK: - Reset

    private func resetPendingState() {
        commitTimer?.invalidate()
        commitTimer = nil
        pendingTrackID = nil
        pendingSong = nil
        pendingTrackStartedAt = nil
        accumulatedSeconds = 0
        isTrackPaused = false
        playbackStartedAt = nil
        requiredListenSeconds = 0
    }

    // MARK: - 75% Threshold

    private func scheduleCommit(for song: Song, remainingSeconds: Double) {
        guard isLoggingEnabled else {
            print("[PlayLogger] Logging disabled — skipping \(song.title)")
            return
        }

        let durationSeconds = song.duration ?? 0
        guard durationSeconds > 0 else {
            Task { await commit(song: song) }
            return
        }

        requiredListenSeconds = durationSeconds * 0.75
        let threshold = max(remainingSeconds, 0)

        print("[PlayLogger] \(song.title) — commit in \(Int(threshold))s")

        commitTimer = Timer.scheduledTimer(withTimeInterval: threshold, repeats: false) { [weak self] _ in
            Task { @MainActor [weak self] in
                guard let self else { return }
                guard let entry = self.player.queue.currentEntry,
                      let item = entry.item,
                      case .song(let currentSong) = item,
                      currentSong.id.rawValue == self.pendingTrackID
                else {
                    print("[PlayLogger] Track changed before 75% — skipping")
                    return
                }
                await self.commit(song: currentSong)
            }
        }
    }

    // MARK: - Commit

    private func commit(song: Song) async {
        accumulatedSeconds = 0
        isTrackPaused = false

        // Format release date as ISO8601 date string if available
        let releaseDate: String?
        if let date = song.releaseDate {
            let formatter = DateFormatter()
            formatter.dateFormat = "yyyy-MM-dd"
            releaseDate = formatter.string(from: date)
        } else {
            releaseDate = nil
        }

        let event = PlayEvent(
            trackId:     song.id.rawValue,
            trackName:   song.title,
            artistName:  song.artistName,
            albumName:   song.albumTitle ?? "",
            durationMs:  Int((song.duration ?? 0) * 1000),
            playedAt:    iso8601.string(from: pendingTrackStartedAt ?? Date()),
            releaseDate: releaseDate,
            latitude:    location.current?.coordinate.latitude,
            longitude:   location.current?.coordinate.longitude
        )

        print("[PlayLogger] ✅ committed: \(event.trackName) by \(event.artistName)")

        LocalStore.append(event)
        recentEvents.insert(event, at: 0)
        if recentEvents.count > 100 { recentEvents.removeLast() }
        await sender.send(event)
    }

    // MARK: - Clear

    // MARK: - Controls

    func toggleLogging() {
        isLoggingEnabled.toggle()
        if !isLoggingEnabled {
            // Cancel any pending commit immediately
            commitTimer?.invalidate()
            commitTimer = nil
            pendingTrackID = nil
            pendingSong = nil
            accumulatedSeconds = 0
            isTrackPaused = false
            playbackStartedAt = nil
        }
        print("[PlayLogger] Logging \(isLoggingEnabled ? "enabled" : "disabled")")
    }

    func clearRecent() {
        recentEvents.removeAll()
        LocalStore.save([])
    }
}
