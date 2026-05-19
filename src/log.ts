import { createClog, type Logger } from "@marianmeres/clog";

/** Package-namespaced logger. All framework code emits via this. */
export const clog: Logger = createClog("workflow");
