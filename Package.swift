// swift-tools-version:6.0
import PackageDescription

// Library only. The runnable demo lives in a separate repository
// (durable-saga-demo-app) that consumes this package as a remote,
// version-pinned dependency. There is deliberately no executable target here.
let package = Package(
    name: "DurableSaga",
    platforms: [
        .iOS(.v16),
        .macOS(.v13)
    ],
    products: [
        .library(name: "DurableSaga", targets: ["DurableSaga"])
    ],
    targets: [
        .target(name: "DurableSaga"),
        .testTarget(name: "DurableSagaTests", dependencies: ["DurableSaga"])
    ]
)
