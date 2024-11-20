/// This is a build script that generates Rust code from Protocol Buffer definitions.
/// It runs during the compilation process, before the main crate is built.
///
/// The script:
/// 1. Configures the tonic-build code generator
/// 2. Generates both client and server gRPC code
/// 3. Outputs the generated files to src/generated/
/// 4. Compiles all .proto files in src/proto/
///
/// Arguments for compile():
/// - First argument: paths to .proto files that should be compiled
/// - Second argument: include paths for resolving proto imports
///
/// The generated code will be used by the proto crate to provide
/// gRPC service definitions and message types to the rest of the application.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true) // Generate server implementations
        .build_client(true) // Generate client implementations
        .out_dir("src/generated") // Output directory for generated files
        .compile(
            &[
                "src/proto/common.proto",   // Common types used across services
                "src/proto/storage.proto",  // Storage service definitions
                "src/proto/metadata.proto", // Metadata service definitions
            ],
            &["src/proto"], // Include path for resolving imports
        )?;
    Ok(())
}
