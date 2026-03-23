import Foundation

/// Persists events to disk before sending.
/// If the POST fails (no signal, server down), events survive here
/// and are retried next time the app launches.
enum LocalStore {
    private static let fileName = "play_events.json"

    private static var fileURL: URL {
        FileManager.default
            .urls(for: .documentDirectory, in: .userDomainMask)[0]
            .appendingPathComponent(fileName)
    }

    private static let encoder: JSONEncoder = JSONEncoder()
    private static let decoder: JSONDecoder = JSONDecoder()

    static func load() -> [PlayEvent] {
        guard let data = try? Data(contentsOf: fileURL),
              let events = try? decoder.decode([PlayEvent].self, from: data)
        else { return [] }
        return events
    }

    static func append(_ event: PlayEvent) {
        var events = load()
        events.insert(event, at: 0)
        if events.count > 2000 { events = Array(events.prefix(2000)) }
        save(events)
    }

    static func save(_ events: [PlayEvent]) {
        if let data = try? encoder.encode(events) {
            try? data.write(to: fileURL, options: .atomic)
        }
    }
}
