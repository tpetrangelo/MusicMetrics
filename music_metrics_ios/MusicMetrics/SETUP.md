# MusicLogger iOS App — Setup

## What this app does
- Observes Apple Music for track changes (screen locked, background, all good)
- Grabs GPS on every track change
- POSTs { track info + lat/lng } to your FastAPI endpoint
- Buffers locally if network is down — retries on next launch
- That's it. Weather, enrichment, dbt — all handled by your pipeline on your laptop.

## Files
```
MusicLoggerApp.swift   — app entry point
PlayEvent.swift        — the data model (6 fields, snake_case for FastAPI)
PlayLogger.swift       — observes SystemMusicPlayer, coordinates everything
LocationManager.swift  — always-on CoreLocation
EventSender.swift      — POSTs to your endpoint
LocalStore.swift       — write-ahead local buffer
ContentView.swift      — minimal UI, sanity check only
```

## Xcode Setup (one time)

### 1. New Project
File → New → App
- Product Name: MusicLogger
- Interface: SwiftUI
- Language: Swift

### 2. Add Files
Delete the default ContentView.swift Xcode creates.
Drag all 7 .swift files into the Project Navigator.

### 3. Capabilities
Click your app target → Signing & Capabilities → "+" for each:

| Capability | Settings |
|---|---|
| MusicKit | Just add it |
| Background Modes | ✅ Audio, AirPlay, Picture in Picture |
| | ✅ Background fetch |
| | ✅ Location updates |

### 4. Info.plist
Right-click Info.plist → Open As → Source Code
Add inside the root <dict>:

```xml
<key>NSMusicUsageDescription</key>
<string>Logs your listening history for personal analytics</string>

<key>NSLocationAlwaysAndWhenInUseUsageDescription</key>
<string>Records location when tracks play for personal analytics</string>

<key>NSLocationWhenInUseUsageDescription</key>
<string>Records location when tracks play for personal analytics</string>

<key>UIBackgroundModes</key>
<array>
    <string>audio</string>
    <string>fetch</string>
    <string>location</string>
</array>
```

### 5. Configure Your Endpoint
Open EventSender.swift — change this line:
```swift
private let endpointURL = URL(string: "https://your-ingestion-endpoint.com/plays")!
```

During local dev, find your Mac's local IP (System Settings → Wi-Fi → Details)
and use: `"http://192.168.1.YOUR_IP:8000/plays"`

### 6. Run
- Plug iPhone into Mac via USB
- Select your iPhone as run target (top bar in Xcode)
- Hit ⌘R
- Accept Music + Location permissions on your phone
  → Location: choose "Always Allow"
- Start playing in Apple Music
- Switch to MusicLogger — events should appear

## Day-to-day
- Open app once after phone restart
- Never force-quit it (swipe up in multitasking)
- Green dot = logging, events posting to your endpoint
- Everything else happens in your pipeline on your laptop
