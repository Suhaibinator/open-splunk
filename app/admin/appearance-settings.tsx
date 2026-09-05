"use client";

import { useCallback, useEffect, useRef, useState, type FormEvent } from "react";

import type {
  GetServerAppearanceResponse,
  UpdateServerAppearanceResponse,
} from "@/gen/ts/open_splunk/server_settings_api";
import { isHttpStatus, paletteFromProto, paletteToProto, type OpenSplunkApiClient } from "@/lib/api";
import { createErrorMessage } from "@/lib/error-message";
import type { Palette } from "@/lib/palettes";
import { applyInstancePalette } from "@/lib/theme-preference";

import { PALETTE_OPTIONS, paletteOptionId, paletteOptions } from "./appearance-form";

/** The saved appearance as the card holds it: the version the next update must name, and two palettes. */
export interface AppearanceModel {
  defaultPalette: Palette;
  palette: Palette;
  version: bigint;
}

/** The card's copy, exported so the markup test and the read-only row can quote it. */
export const APPEARANCE_TITLE = "Appearance";
export const APPEARANCE_DESCRIPTION =
  "Instance-wide palette shown to every user, including the sign-in page. Light and dark stay each user's own choice.";

/**
 * Narrows a get or update envelope to the model, or `null` when the server
 * left out the versioned current value. An unknown palette on the wire paints
 * classic, the same as bootstrap does, so a newer server never blanks the card.
 */
export function adaptAppearance(
  response: GetServerAppearanceResponse | UpdateServerAppearanceResponse,
): AppearanceModel | null {
  if (response.current === undefined) return null;
  return {
    defaultPalette: paletteFromProto(response.defaultPalette),
    palette: paletteFromProto(response.current.palette),
    version: response.current.version,
  };
}

/**
 * The card itself, with no state of its own: `saved` is what the server
 * holds, `selected` what the administrator has clicked, and the difference
 * between the two is the dirty flag. Kept free of effects so the markup can
 * be rendered statically in a test.
 */
export function AppearanceCard({
  busy,
  defaultPalette,
  onChoose,
  saved,
  selected,
}: {
  busy: boolean;
  defaultPalette: Palette;
  onChoose: (palette: Palette) => void;
  saved: Palette;
  selected: Palette;
}) {
  const dirty = selected !== saved;
  return (
    <>
      <section className="suite-card settings-group">
        <header><h3>{APPEARANCE_TITLE}</h3><p>{APPEARANCE_DESCRIPTION}</p></header>
        <div className="appearance-palette-options" role="radiogroup" aria-label="Palette">
          {paletteOptions().map(([palette, option]) => {
            const id = paletteOptionId(palette);
            const checked = selected === palette;
            return (
              <label className={checked ? "is-selected" : undefined} htmlFor={id} key={palette}>
                <input
                  aria-describedby={`${id}-description`}
                  aria-label={option.label}
                  checked={checked}
                  disabled={busy}
                  id={id}
                  name="appearance-palette"
                  onChange={() => onChoose(palette)}
                  type="radio"
                  value={palette}
                />
                <span>
                  <strong>{option.label}</strong>
                  <small id={`${id}-description`}>{option.description}</small>
                </span>
              </label>
            );
          })}
        </div>
      </section>
      <div className="settings-actions">
        <button
          className="button button--secondary"
          disabled={busy || selected === defaultPalette}
          onClick={() => onChoose(defaultPalette)}
          type="button"
        >
          Reset to default
        </button>
        <button className="button button--primary" disabled={busy || !dirty} type="submit">
          {busy ? "Applying…" : "Apply"}
        </button>
      </div>
    </>
  );
}

/**
 * The Appearance form: loads the saved palette, previews a click live on
 * this document, and writes the choice back under the version it loaded.
 *
 * The preview goes through `applyInstancePalette`, the same call the live
 * bootstrap uses, so what the administrator sees is exactly what every user
 * will get. Because that call also caches the value for the next boot, the
 * saved palette is restored on every path that abandons the preview: a
 * reload, a 409 conflict, and unmount.
 */
export function AppearanceSettings({
  client,
  onStatus,
  onDirtyChange,
}: {
  client: OpenSplunkApiClient;
  onStatus: (message: string, kind: "success" | "warning") => void;
  onDirtyChange: (dirty: boolean) => void;
}) {
  const [saved, setSaved] = useState<AppearanceModel | null>(null);
  const [selected, setSelected] = useState<Palette | null>(null);
  const [state, setState] = useState<"loading" | "ready" | "saving" | "error">("loading");
  const [error, setError] = useState<string | null>(null);
  const [activeClient, setActiveClient] = useState(client);
  if (activeClient !== client) {
    setActiveClient(client);
    setState("loading");
    setError(null);
  }

  // The palette to restore when the preview is abandoned. A ref rather than
  // the state itself because the unmount cleanup below has to read the value
  // current at unmount, not the one captured when the effect was registered.
  const savedPalette = useRef<Palette | null>(null);
  useEffect(() => {
    savedPalette.current = saved?.palette ?? null;
  }, [saved]);
  useEffect(() => () => {
    const palette = savedPalette.current;
    if (palette !== null) applyInstancePalette(palette);
  }, []);

  const adopt = useCallback((model: AppearanceModel) => {
    setSaved(model);
    setSelected(model.palette);
    setState("ready");
    applyInstancePalette(model.palette);
  }, []);

  const load = useCallback(async () => {
    try {
      const next = adaptAppearance(await client.serverSettings.getAppearance({}));
      if (next === null) throw new Error("The server returned incomplete appearance settings.");
      adopt(next);
      return true;
    } catch (cause) {
      setError(createErrorMessage("Appearance settings could not be loaded.")(cause));
      setState("error");
      return false;
    }
  }, [adopt, client]);

  const reload = useCallback(() => {
    setState("loading");
    setError(null);
    return load();
  }, [load]);

  useEffect(() => {
    if (activeClient !== client) return;
    let current = true;
    void client.serverSettings.getAppearance({}).then((response) => {
      if (!current) return;
      const next = adaptAppearance(response);
      if (next === null) throw new Error("The server returned incomplete appearance settings.");
      adopt(next);
    }).catch((cause: unknown) => {
      if (!current) return;
      setError(createErrorMessage("Appearance settings could not be loaded.")(cause));
      setState("error");
    });
    return () => {
      current = false;
    };
  }, [activeClient, adopt, client]);

  const dirty = saved !== null && selected !== null && selected !== saved.palette;
  useEffect(() => {
    onDirtyChange(dirty);
    return () => onDirtyChange(false);
  }, [dirty, onDirtyChange]);
  useEffect(() => {
    if (!dirty) return;
    const protect = (event: BeforeUnloadEvent) => event.preventDefault();
    window.addEventListener("beforeunload", protect);
    return () => window.removeEventListener("beforeunload", protect);
  }, [dirty]);

  if (state === "error") {
    return <div className="access-mode-notice" role="alert"><span>!</span><div><strong>Appearance could not be loaded</strong><p>{error}</p><button type="button" onClick={() => void reload()}>Retry</button></div></div>;
  }
  if (state === "loading" || saved === null || selected === null) {
    return <output className="access-mode-notice"><span>i</span><div><strong>Loading appearance</strong><p>Reading the current palette and its version…</p></div></output>;
  }

  const choose = (palette: Palette) => {
    setSelected(palette);
    applyInstancePalette(palette);
  };

  const save = async (event: FormEvent) => {
    event.preventDefault();
    if (!dirty) return;
    setState("saving");
    try {
      const next = adaptAppearance(await client.serverSettings.updateAppearance({
        expectedVersion: saved.version,
        palette: paletteToProto(selected),
      }));
      if (next === null) throw new Error("The server returned incomplete appearance settings.");
      adopt(next);
      onStatus(`Palette set to ${PALETTE_OPTIONS[next.palette].label}. Every user sees it on their next page load.`, "success");
    } catch (cause) {
      if (isHttpStatus(cause, 409)) {
        applyInstancePalette(saved.palette);
        const reloaded = await reload();
        onStatus(reloaded
          ? "The palette changed on the server. The latest version was reloaded; review it before applying again."
          : "The palette changed on the server, and the latest version could not be reloaded.", "warning");
        return;
      }
      setState("ready");
      onStatus(createErrorMessage("The palette could not be updated.")(cause), "warning");
    }
  };

  return (
    <form className="admin-section-stack" onSubmit={(event) => void save(event)} noValidate>
      <AppearanceCard
        busy={state === "saving"}
        defaultPalette={saved.defaultPalette}
        onChoose={choose}
        saved={saved.palette}
        selected={selected}
      />
    </form>
  );
}
