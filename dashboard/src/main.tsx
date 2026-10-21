import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App";
import "./styles.css";
import "./v3/FourPage.css";
import { localOwnerControlUrl } from "./v3/controlOrigin";

const controlUrl = localOwnerControlUrl(window.location.href, import.meta.env.VITE_V3_OWNER_CONTROL === "1");
if (controlUrl) window.location.replace(controlUrl);
else createRoot(document.getElementById("root")!).render(<StrictMode><App /></StrictMode>);
