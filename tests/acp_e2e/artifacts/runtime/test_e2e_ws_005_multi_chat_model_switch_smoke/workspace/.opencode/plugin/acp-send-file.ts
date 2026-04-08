import path from "path"
import { stat } from "fs/promises"
import { pathToFileURL } from "url"

import { Plugin, tool } from "@opencode-ai/plugin"

function mime(file: string) {
  const ext = path.extname(file).toLowerCase()
  if (ext === ".png") return "image/png"
  if (ext === ".jpg" || ext === ".jpeg") return "image/jpeg"
  if (ext === ".gif") return "image/gif"
  if (ext === ".webp") return "image/webp"
  if (ext === ".svg") return "image/svg+xml"
  if (ext === ".pdf") return "application/pdf"
  if (ext === ".json") return "application/json"
  if (ext === ".csv") return "text/csv"
  if (ext === ".md") return "text/markdown"
  if (ext === ".txt" || ext === ".log") return "text/plain"
  return "application/octet-stream"
}

const plugin: Plugin = async (input) => ({
  tool: {
    acp_send_file: tool({
      args: {
        filePath: tool.schema.string(),
        filename: tool.schema.string().optional(),
        mimeType: tool.schema.string().optional(),
      },
      async execute(args, ctx) {
        const file = path.isAbsolute(args.filePath)
          ? args.filePath
          : path.resolve(ctx.directory, args.filePath)
        const info = await stat(file)
        if (!info.isFile()) {
          throw new Error(`Expected file, got non-file path: ${file}`)
        }
        await ctx.ask({ permission: "read", patterns: [file], always: ["*"] })
        return `Queued file for ACP resource_link: ${path.basename(file)}`
      },
    }),
  },
  async "tool.execute.after"(evt, out) {
    const filePath = typeof evt.args?.filePath === "string" ? evt.args.filePath : ""
    if (!filePath) return
    const file = path.isAbsolute(filePath) ? filePath : path.resolve(input.directory, filePath)
    const info = await stat(file).catch(() => undefined)
    if (!info?.isFile()) {
      out.output = `acp_send_file failed: file not found (${file})`
      return
    }
    const filename =
      typeof evt.args?.filename === "string" && evt.args.filename.trim().length > 0
        ? evt.args.filename
        : path.basename(file)
    const type =
      typeof evt.args?.mimeType === "string" && evt.args.mimeType.trim().length > 0
        ? evt.args.mimeType
        : mime(file)
    ;(out as any).attachments = [
      ...(((out as any).attachments ?? []) as any[]),
      {
        type: "file",
        filename,
        mime: type,
        url: pathToFileURL(file).href,
      },
    ]
    out.output = `Queued file for ACP resource_link: ${filename}`
  },
})

export default plugin
