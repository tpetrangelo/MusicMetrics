import SwiftUI

struct ContentView: View {
    @StateObject private var logger = PlayLogger.shared
    @State private var showingClearConfirm = false

    var body: some View {
        NavigationStack {
            List {
                // Status
                Section {
                    HStack(spacing: 12) {
                        Circle()
                            .fill(logger.isLoggingEnabled ? .green : .red)
                            .frame(width: 10, height: 10)
                        VStack(alignment: .leading, spacing: 2) {
                            Text(logger.isLoggingEnabled ? "Logging active" : "Logging paused")
                                .font(.headline)
                            Text(logger.isLoggingEnabled
                                 ? "Playing Apple Music normally — this runs in the background"
                                 : "No plays will be logged or posted to your endpoint")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                        Spacer()
                        Toggle("", isOn: Binding(
                            get: { logger.isLoggingEnabled },
                            set: { _ in logger.toggleLogging() }
                        ))
                        .labelsHidden()
                    }
                    .padding(.vertical, 4)
                }

                // Recent events
                Section(header: Text("Recent (\(logger.recentEvents.count))")) {
                    if logger.recentEvents.isEmpty {
                        Text("No plays yet — start playing music")
                            .foregroundStyle(.secondary)
                            .font(.subheadline)
                    } else {
                        let events: [PlayEvent] = Array(logger.recentEvents.prefix(30))
                        ForEach(events) { (event: PlayEvent) in
                            VStack(alignment: .leading, spacing: 4) {
                                Text(event.trackName).font(.headline).lineLimit(1)
                                Text(event.artistName).font(.subheadline)
                                    .foregroundStyle(.secondary).lineLimit(1)
                                HStack(spacing: 10) {
                                    Label(event.playedAt, systemImage: "clock")
                                    if let lat = event.latitude, let lon = event.longitude {
                                        Label(
                                            String(format: "%.3f, %.3f", lat, lon),
                                            systemImage: "location.fill"
                                        )
                                        .foregroundStyle(.blue)
                                    }
                                }
                                .font(.caption)
                                .foregroundStyle(.secondary)
                            }
                            .padding(.vertical, 2)
                        }
                    }
                }
            }
            .navigationTitle("Music Metrics")
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button(role: .destructive) {
                        showingClearConfirm = true
                    } label: {
                        Label("Clear", systemImage: "trash")
                    }
                    .disabled(logger.recentEvents.isEmpty)
                }
            }
            .confirmationDialog(
                "Clear all recent plays?",
                isPresented: $showingClearConfirm,
                titleVisibility: .visible
            ) {
                Button("Clear", role: .destructive) {
                    logger.clearRecent()
                }
                Button("Cancel", role: .cancel) {}
            } message: {
                Text("This only clears the local display — data already sent to your endpoint is unaffected.")
            }
        }
    }
}
