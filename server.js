// Convenience entrypoint. Each service has its own entrypoint under
// src/services/ and runs in its own container; this file just starts the API so
// that `npm start` still does the obvious thing.
require("./src/services/api");
