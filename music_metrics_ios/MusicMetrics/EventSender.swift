import Foundation

/// Single responsibility: take a PlayEvent and POST it to your ingestion endpoint.
/// The app doesn't know or care what happens after this.
class EventSender {
    static let shared = EventSender()

    // ⚙️ Point this at your FastAPI server
    // Local dev:   "http://192.168.1.x:8000/plays"  (your Mac's local IP)
    // Production:  "https://your-app.railway.app/plays"
    private let endpointURL = URL(string: 
"https://ECSKEY.execute-api.us-east-1.amazonaws.com/plays/ios")!

    private let encoder: JSONEncoder = {
        let e = JSONEncoder()
        return e
    }()

    func send(_ event: PlayEvent) async {
//            // Temp: just print to console instead of POSTing
//            if let data = try? JSONEncoder().encode(event),
//               let json = String(data: data, encoding: .utf8) {
//                print("[MusicMetrics] New play event:")
//                print(json)
//            }
        guard let body = try? encoder.encode(event) else { return }

        var request = URLRequest(url: endpointURL)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = body
        request.timeoutInterval = 10

        do {
            let (_, response) = try await URLSession.shared.data(for: request)
            if let http = response as? HTTPURLResponse, http.statusCode != 200 {
                // Non-200 — event is already in LocalStore, will retry on next launch
                print("[EventSender] Non-200 response: \(http.statusCode)")
            }
        } catch {
            // Network failure — event is already in LocalStore, will retry on next launch
            print("[EventSender] Failed to send: \(error.localizedDescription)")
        }
    }

    /// Call on app launch to retry anything that failed to send previously
    func retryPending() async {
        let pending = LocalStore.load()
        guard !pending.isEmpty else { return }
        for event in pending {
            await send(event)
        }
    }
}
