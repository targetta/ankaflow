Pipeline API provides building blocks for YAML pipeline.

Each stage and configuration object here represents a specific part of the YAML definition file.

Example pipeline stage with all possible keys. Actual usage of keys depends on used model `kind`.

```
- name: str
  kind: source | sink | transform
  log_level: DEBUG
  skip_if: 
  on_error: error | continue
  connection: Connection
    kind: Parquet | Deltatable | Rest | ...
    locator: str
    config: ConnectionConfiguration
    client: RestClient
    request: RestRequest
      endpoint: str
      method: RequestMethod
      errorhandler: RestErrorHandler
      auth: RestAuth
        method: header | oauth2 | ...
        values: StringDict
      query: dict | @json-magic
      body: dict | @json-magic
      response: RestResponse
        handler: 
            kind: ResponseHandlerTypes
            page_param: str
            page_size: str
            param_locator: ParameterDisposition
            total_records: str
            throttle: int | float 
        content_type: DataType
        locator: str
    fields: Fields
  show: int
  show_schema: bool
  query: > str
  stages: Stages
  throttle: int
        
```

::: ankaflow.models