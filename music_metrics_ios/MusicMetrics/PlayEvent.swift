import Foundation

/// The only thing the app is responsible for knowing.
/// No weather. No enrichment. Just what happened, when, and where.
struct PlayEvent: Codable, Identifiable {
    let id: UUID
    let trackId: String
    let trackName: String
    let artistName: String
    let albumName: String
    let durationMs: Int
    let playedAt: String      // ISO8601 string — easy to parse anywhere
    let releaseDate: String?  // ISO8601 date string e.g. "2024-03-15"
    let latitude: Double?
    let longitude: Double?

    init(trackId: String, trackName: String, artistName: String,
         albumName: String, durationMs: Int, playedAt: String,
         releaseDate: String?, latitude: Double?, longitude: Double?) {
        self.id          = UUID()
        self.trackId     = trackId
        self.trackName   = trackName
        self.artistName  = artistName
        self.albumName   = albumName
        self.durationMs  = durationMs
        self.playedAt    = playedAt
        self.releaseDate = releaseDate
        self.latitude    = latitude
        self.longitude   = longitude
    }

    // Encoding uses snake_case to match your FastAPI schema
    enum CodingKeys: String, CodingKey {
        case id
        case trackId        = "track_id"
        case trackName      = "track_name"
        case artistName     = "artist_name"
        case albumName      = "album_name"
        case durationMs     = "duration_ms"
        case playedAt       = "played_at"
        case releaseDate    = "release_date"
        case latitude
        case longitude
    }
}
