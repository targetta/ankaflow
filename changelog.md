# Changelog:

## [0.5.0] - 2026-03-21
This release introduces a multi-layered security architecture designed to establish a **"Logical Air-gap"** between template execution and the host system. These changes prevent Remote Code Execution (RCE) and protect sensitive environment data from leaking through the rendering engine.

### Security Note: The "Logical Air-gap"
We have implemented a tiered defense strategy to isolate the template "Detonation Chamber." This ensures that even if a template author attempts to probe the system, they are restricted to a sanitized data environment.

### New Behavior
* **The Dunder-Gate:** All "dunder" attributes (variables starting and ending with double underscores, e.g., `__typename__`, `__version__`) are now **strictly prohibited** at the container level.
* **Environment Masking:** A global hook now intercepts calls to `os.getenv` and `os.environ` during the rendering lifecycle. Environment masking can be enabled by setting environment variable `ANKAFLOW_SECURITY=1`.
* **Hardened Sandbox:** The engine now utilizes a `StrictEnvironment` (based on `Jinja2.SandboxedEnvironment`) which clears Python built-ins and restricts attribute traversal.

### Breaking Changes
* **Inaccessible Internal Attributes:** Any template attempting to access a dunder attribute will now trigger a `KeyError` or `AttributeError`. 
    * *Impact:* Code like `{{ vars.__class__ }}` or `{{ vars.__typename__ }}` will no longer function.
* **CLI Auto-Initialization:** The **AnkaFlow CLI** now automatically imports the security module and initializes environment masking. No user action is required for CLI usage.

### Library Impact & Integration
For developers using this project as a library/dependency, the security behavior is **opt-in** for the global environment hooks:

* **Self-Contained Protections (Always On):** The **Dunder-Gate** and **Jinja Sandbox** are built into the `BaseSafeDict` and `Renderer` classes. These will function regardless of how the library is imported.
* **Global Masking (Optional/Recommended):** The `os.environ` protection requires a global monkeypatch to be installed at the application entry point.
* **Risk of Non-Initialization:** If a parent project calls the `Renderer` without initializing security, the `os` module remains unmasked. If a template manages to access `os` via an object leak, it could potentially read the host's real environment variables (e.g., `API_KEY`).

### Recommendation for Parent Projects
To ensure full "Logical Air-gap" protection in your own applications, you **must** explicitly enable the security layer by setting environment variable:

`ANKAFLOW_SECURITY=1`

### Deprecations
*- **ImmutableMap:** Previously base class for `FlowContext`. Deprecated in favor of `BaseSafeDict`.
