import { clsx, type ClassValue } from "clsx";
import { extendTailwindMerge } from "tailwind-merge";

// The type roles are font sizes: without this, the merge takes
// `text-ui-body` for a colour and drops the real colour class.
const merge = extendTailwindMerge({
  extend: {
    classGroups: {
      "font-size": [{ text: ["2xs", "ui-page-title", "ui-detail-title", "ui-heading", "ui-section", "ui-body", "ui-label", "ui-caption", "ui-badge", "ui-code"] }],
    },
  },
});

export function cn(...inputs: ClassValue[]): string {
  return merge(clsx(inputs));
}
