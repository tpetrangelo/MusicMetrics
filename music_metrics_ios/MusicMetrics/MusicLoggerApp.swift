import SwiftUI
import MusicKit

@main
struct MusicLoggerApp: App {
    @UIApplicationDelegateAdaptor(AppDelegate.self) var appDelegate

    var body: some Scene {
        WindowGroup {
            ContentView()
                .task {
                    await MusicAuthorization.request()
                    await PlayLogger.shared.start()
                }
        }
    }
}

class AppDelegate: NSObject, UIApplicationDelegate {
    func application(
        _ application: UIApplication,
        didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]? = nil
    ) -> Bool {
        // If iOS killed and relaunched the app due to a significant
        // location change, restart the logger immediately
        if launchOptions?[.location] != nil {
            print("[AppDelegate] Relaunched by iOS due to location event — restarting logger")
            Task { await PlayLogger.shared.start() }
        }
        return true
    }
}
