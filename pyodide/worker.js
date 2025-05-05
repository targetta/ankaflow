self.addEventListener("message", async (event) => {
  if (event.data.type === "init") {
    const projectResponse = await fetch("package.json");
    const projectConfig = await projectResponse.json();

    importScripts(projectConfig.pyodide_url || "https://cdn.jsdelivr.net/pyodide/v0.27.4/full/pyodide.js");
    importScripts("https://cdn.jsdelivr.net/npm/axios@1.6.7/dist/axios.min.js");

    self.pyodide = await loadPyodide();

    await self.pyodide.loadPackage(projectConfig.dependencies);

    if (projectConfig.wheel_url) {
        const wheelURL = `${projectConfig.base_url}/${projectConfig.wheel_url}`;
        await self.pyodide.runPythonAsync(`
                import micropip
                await micropip.install("${wheelURL}")
            `);
    } else {
        await self.pyodide.runPythonAsync(`
                import micropip
                await micropip.install("${projectConfig.package}")
            `);
    }

    const mainScriptResponse = await fetch("main_code.py");
    const mainScript = await mainScriptResponse.text();
    await self.pyodide.runPythonAsync(mainScript);

    self.postMessage({ type: "ready" });
  }

  if (event.data.type === "execute") {
    if (!self.pyodide) {
      self.postMessage({ type: "error", message: "Pyodide not initialized" });
      return;
    }

    try {
      const { id, yaml, env } = event.data;
      const encodedResult = await self.pyodide.runPythonAsync(`
          await main(${JSON.stringify(yaml)}, ${JSON.stringify(env)})
        `);
      const logs = JSON.parse(encodedResult);
      self.postMessage({ type: "result", id, logs });
    } catch (err) {
      self.postMessage({ type: "error", error: err.toString() });
    }
  }
});

// Example of using Axios for advanced features.
async function getHTTPResponse(url, argsJson) {
  try {
      argsJson = argsJson ? argsJson : {};
      const args = JSON.parse(argsJson);
      // Map args.body → args.data
      if ("body" in args) {
          args.data = args.body;
          delete args.body;
      }
      args.responseType = 'arraybuffer';
      // Axios call using the provided URL and arguments
      const axiosResponse = await axios(url, args);
      // Mimicking a `Response` object similar to pyfetch
      const response = {
          body: axiosResponse.data,
          get status() {
              return axiosResponse.status
          },
          get url() { return axiosResponse.config.url },
          get encoding() {
              const contentType = axiosResponse.headers['content-type'] || '';
              const charsetMatch = contentType.match(/charset=([a-zA-Z0-9\-]+)/);
              return charsetMatch ? charsetMatch[1] : 'utf-8';  // Default to utf-8 if not found
          },
          get ok() { return axiosResponse.status >= 200 && axiosResponse.status < 300 },
          // Mimicking .json() and .text() methods
          json: async function () {
              if (axiosResponse.data instanceof ArrayBuffer) {
                  // Try to detect if it's JSON content (based on content type or response data)
                  console.log(axiosResponse.data)
                  try {
                      const decoder = new TextDecoder('utf-8');
                      const text = decoder.decode(axiosResponse.data); // Convert ArrayBuffer to text
                      return text;  // Return the raw string representation of JSON
                  } catch (e) {
                      throw new Error('Unable to parse ArrayBuffer as JSON');
                  }
              }
              // In case the response is already in text format, just return it directly
              return axiosResponse.data;
          },
          text: async function () {
              return await this.json();
          },
          // Mimicking .bytes() method for binary responses
          bytes: async function () {
              // If the response is binary (ArrayBuffer), return it as a Uint8Array
              if (axiosResponse.data instanceof ArrayBuffer) {
                  return new Uint8Array(axiosResponse.data);
              }
              throw new Error('Response body is not binary data');
          }
      };

      // Return the mimicked response object
      return response;
  } catch (error) {
      console.log(error)
      if (!error.response) {
        throw new Error(`Network failure: ${error.message}`);
      }
      // Handle any axios-specific errors (e.g., network issues, response errors)
      response = {
          error() { return error.message },
          get status() { return error.response ? error.response.status : 500 },
          get ok() { return false },
          get url() { return url },
          text: async function () {
              return await error.response.text();
          },
          json: async function () {
              if (error.response.data instanceof ArrayBuffer) {
                  // Try to detect if it's JSON content (based on content type or response data)
                  console.log(error.response.data)
                  try {
                      const decoder = new TextDecoder('utf-8');
                      const text = decoder.decode(error.response.data); // Convert ArrayBuffer to text
                      return text;  // Return the raw string representation of JSON
                  } catch (e) {
                      throw new Error('Unable to parse ArrayBuffer as JSON');
                  }
              }
              // In case the response is already in text format, just return it directly
              return error.response.data;
          },
          text: async function () {
              return await this.json();
          }
      };
      return response
  }
}