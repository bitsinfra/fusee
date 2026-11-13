import { createRoot } from "react-dom/client";
import { App } from "./app";
import { apply, watchSystem } from "./lib/theme";
import "./lib/api";

apply();
watchSystem();
const root = document.getElementById("root");
if (root) createRoot(root).render(<App />);
