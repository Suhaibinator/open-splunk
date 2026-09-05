/**
 * A document small enough to read in one sitting, for mounting a component
 * with `react-dom/client` under `node:test`.
 *
 * The unit tests render markup statically, which runs no effects and no
 * handlers. A component whose point is what happens *after* a click (a live
 * preview, a restore on unmount, one branch for a 409 and another for the
 * rest) needs a real render root, and a real render root needs a host: the
 * handful of DOM members react-dom reads and writes -- a tree, attributes, a
 * few reflected input properties, listeners, `querySelectorAll` for radio
 * groups -- plus the slice of `window` that `lib/theme-preference.ts` reads.
 * Nothing here lays out, styles, or navigates; it is a mock, not a browser,
 * and it is kept to what the tests that import it exercise.
 *
 * Test-only: no page or library module imports this file.
 */

type Listener = (event: FakeEvent) => void;

/** The event object a fake dispatch carries; the fields react-dom reads. */
export interface FakeEvent {
  currentTarget: FakeNode | null;
  defaultPrevented: boolean;
  preventDefault(): void;
  propagationStopped: boolean;
  stopPropagation(): void;
  target: FakeNode | null;
  type: string;
}

/** Builds an event to hand to `FakeNode.dispatchEvent` or `FakeBrowser.dispatchWindowEvent`. */
export function fakeEvent(type: string): FakeEvent {
  return {
    currentTarget: null,
    defaultPrevented: false,
    preventDefault() {
      this.defaultPrevented = true;
    },
    propagationStopped: false,
    stopPropagation() {
      this.propagationStopped = true;
    },
    target: null,
    type,
  };
}

/** The node and every ancestor, nearest first. */
function ancestry(node: FakeNode): FakeNode[] {
  const path: FakeNode[] = [];
  for (let current: FakeNode | null = node; current !== null; current = current.parentNode) path.push(current);
  return path;
}

/** A `tag`, `#id`, `[attr]` and `[attr="value"]` matcher: the shapes react-dom and the tests use. */
function compileSelector(selector: string): (element: FakeElement) => boolean {
  const tests: Array<(element: FakeElement) => boolean> = [];
  let rest = selector.trim();
  const tag = /^[a-z][a-z0-9-]*/iu.exec(rest);
  if (tag !== null) {
    const name = tag[0].toUpperCase();
    tests.push((element) => element.nodeName === name);
    rest = rest.slice(tag[0].length);
  }
  const id = /^#([\w-]+)/u.exec(rest);
  if (id !== null) {
    tests.push((element) => element.getAttribute("id") === id[1]);
    rest = rest.slice(id[0].length);
  }
  if (!/^(?:\[[\w-]+(?:="[^"]*")?\])*$/u.test(rest)) {
    throw new Error(`fake-dom cannot match the selector ${JSON.stringify(selector)}`);
  }
  for (const [, name, value] of rest.matchAll(/\[([\w-]+)(?:="([^"]*)")?\]/gu)) {
    tests.push((element) => (value === undefined
      ? element.hasAttribute(name)
      : element.getAttribute(name) === value));
  }
  return (element) => tests.every((matches) => matches(element));
}

export class FakeNode {
  public childNodes: FakeNode[] = [];
  public parentNode: FakeNode | null = null;
  public readonly ownerDocument: FakeDocument;
  private readonly listeners = new Map<string, { bubble: Set<Listener>; capture: Set<Listener> }>();

  public constructor(ownerDocument: FakeDocument | null, public readonly nodeType: number, public readonly nodeName: string) {
    this.ownerDocument = ownerDocument ?? (this as unknown as FakeDocument);
  }

  public get firstChild(): FakeNode | null {
    return this.childNodes[0] ?? null;
  }

  public get lastChild(): FakeNode | null {
    return this.childNodes[this.childNodes.length - 1] ?? null;
  }

  public get nextSibling(): FakeNode | null {
    const siblings = this.parentNode?.childNodes ?? [];
    return siblings[siblings.indexOf(this) + 1] ?? null;
  }

  public get previousSibling(): FakeNode | null {
    const siblings = this.parentNode?.childNodes ?? [];
    return siblings[siblings.indexOf(this) - 1] ?? null;
  }

  public get textContent(): string {
    return this.childNodes.map((child) => child.textContent).join("");
  }

  public set textContent(text: string) {
    for (const child of this.childNodes) child.parentNode = null;
    this.childNodes = [];
    if (text !== "") this.appendChild(this.ownerDocument.createTextNode(text));
  }

  public appendChild<T extends FakeNode>(child: T): T {
    return this.insertBefore(child, null);
  }

  public insertBefore<T extends FakeNode>(child: T, before: FakeNode | null): T {
    child.parentNode?.removeChild(child);
    const index = before === null ? this.childNodes.length : this.childNodes.indexOf(before);
    if (index < 0) throw new Error("fake-dom: insertBefore with a reference node that is not a child");
    this.childNodes.splice(index, 0, child);
    child.parentNode = this;
    return child;
  }

  public removeChild<T extends FakeNode>(child: T): T {
    const index = this.childNodes.indexOf(child);
    if (index < 0) throw new Error("fake-dom: removeChild of a node that is not a child");
    this.childNodes.splice(index, 1);
    child.parentNode = null;
    return child;
  }

  public contains(node: FakeNode | null): boolean {
    return node !== null && ancestry(node).includes(this);
  }

  public addEventListener(type: string, listener: Listener | null, options?: boolean | { capture?: boolean }): void {
    if (listener === null) return;
    const phases = this.listeners.get(type) ?? { bubble: new Set(), capture: new Set() };
    this.listeners.set(type, phases);
    const capture = typeof options === "boolean" ? options : options?.capture === true;
    (capture ? phases.capture : phases.bubble).add(listener);
  }

  public removeEventListener(type: string, listener: Listener | null, options?: boolean | { capture?: boolean }): void {
    if (listener === null) return;
    const capture = typeof options === "boolean" ? options : options?.capture === true;
    const phases = this.listeners.get(type);
    (capture ? phases?.capture : phases?.bubble)?.delete(listener);
  }

  /** Capture from the root down to the target, then bubble back up, the way a browser does. */
  public dispatchEvent(event: FakeEvent): boolean {
    event.target = this;
    const path = ancestry(this);
    const deliver = (node: FakeNode, capture: boolean) => {
      const phases = node.listeners.get(event.type);
      const listeners = capture ? phases?.capture : phases?.bubble;
      if (listeners === undefined) return;
      event.currentTarget = node;
      // A copy: a listener may remove itself while the event is delivered.
      for (const listener of Array.from(listeners)) listener(event);
    };
    for (let index = path.length - 1; index >= 0 && !event.propagationStopped; index -= 1) deliver(path[index], true);
    for (let index = 0; index < path.length && !event.propagationStopped; index += 1) deliver(path[index], false);
    event.currentTarget = null;
    return !event.defaultPrevented;
  }

  public querySelector(selector: string): FakeElement | null {
    return this.querySelectorAll(selector)[0] ?? null;
  }

  public querySelectorAll(selector: string): FakeElement[] {
    const matches = compileSelector(selector);
    const found: FakeElement[] = [];
    const walk = (node: FakeNode) => {
      for (const child of node.childNodes) {
        if (child instanceof FakeElement && matches(child)) found.push(child);
        walk(child);
      }
    };
    walk(this);
    return found;
  }
}

export class FakeText extends FakeNode {
  public nodeValue: string;

  public constructor(ownerDocument: FakeDocument, text: string) {
    super(ownerDocument, 3, "#text");
    this.nodeValue = text;
  }

  public override get textContent(): string {
    return this.nodeValue;
  }

  public override set textContent(text: string) {
    this.nodeValue = text;
  }
}

/** The style object react-dom reads at load time; it never has to compute anything. */
class FakeStyle {
  public cssText = "";
  private readonly properties = new Map<string, string>();

  public getPropertyValue(name: string): string {
    return this.properties.get(name) ?? "";
  }

  public removeProperty(name: string): void {
    this.properties.delete(name);
  }

  public setProperty(name: string, value: string): void {
    this.properties.set(name, value);
  }
}

export class FakeElement extends FakeNode {
  public readonly attributes = new Map<string, string>();
  public readonly style = new FakeStyle();
  /** The input state react-dom sets as properties; they do not reflect to attributes, as in a browser. */
  public checked = false;
  public defaultChecked = false;
  public defaultValue = "";
  public value = "";

  public constructor(ownerDocument: FakeDocument, name: string) {
    super(ownerDocument, 1, name.toUpperCase());
  }

  public get tagName(): string {
    return this.nodeName;
  }

  public get id(): string {
    return this.getAttribute("id") ?? "";
  }

  public set id(value: string) {
    this.setAttribute("id", value);
  }

  public get name(): string {
    return this.getAttribute("name") ?? "";
  }

  public set name(value: string) {
    this.setAttribute("name", value);
  }

  public get type(): string {
    return this.getAttribute("type") ?? "";
  }

  public set type(value: string) {
    this.setAttribute("type", value);
  }

  public get disabled(): boolean {
    return this.hasAttribute("disabled");
  }

  public set disabled(value: boolean) {
    if (value) this.setAttribute("disabled", "");
    else this.removeAttribute("disabled");
  }

  public getAttribute(name: string): string | null {
    return this.attributes.get(name) ?? null;
  }

  public hasAttribute(name: string): boolean {
    return this.attributes.has(name);
  }

  public removeAttribute(name: string): void {
    this.attributes.delete(name);
  }

  public setAttribute(name: string, value: unknown): void {
    this.attributes.set(name, String(value));
  }

  public blur(): void {
    if (this.ownerDocument.activeElement === this) this.ownerDocument.activeElement = null;
  }

  public focus(): void {
    this.ownerDocument.activeElement = this;
  }
}

export class FakeDocument extends FakeNode {
  public activeElement: FakeElement | null = null;
  public readonly body: FakeElement;
  public readonly documentElement: FakeElement;
  public readonly head: FakeElement;

  public constructor() {
    super(null, 9, "#document");
    this.documentElement = this.appendChild(this.createElement("html"));
    this.head = this.documentElement.appendChild(this.createElement("head"));
    this.body = this.documentElement.appendChild(this.createElement("body"));
  }

  public createElement(name: string): FakeElement {
    return new FakeElement(this, name);
  }

  public createTextNode(text: string): FakeText {
    return new FakeText(this, text);
  }
}

/** What a test can observe and steer once the fake browser is installed. */
export interface FakeBrowser {
  /** A `<meta name="theme-color">` in the head, as `app/layout.tsx` renders one. */
  chromeMeta: FakeElement;
  document: FakeDocument;
  /** `getComputedStyle(documentElement).getPropertyValue(name)` answers from here, given the element. */
  computedStyle: Map<string, (documentElement: FakeElement) => string>;
  /** Every `localStorage.setItem` in order. */
  storageWrites: Array<[string, string]>;
  storage: Map<string, string>;
  /** Dispatches `type` on `window` (the component's `beforeunload` guard listens there). */
  dispatchWindowEvent(type: string): FakeEvent;
  /** Puts the globals back. */
  uninstall(): void;
  windowListeners: Map<string, Set<Listener>>;
}

/** react-dom walks focus through iframes with an `instanceof` on this; nothing here is one. */
class FakeIFrameElement {
  public readonly contentWindow: null = null;
}

/**
 * Installs `window`, `document`, and `IS_REACT_ACT_ENVIRONMENT` on
 * `globalThis`. Call it before `react-dom/client` is first imported in the
 * process: that module decides at load time whether it can use the DOM.
 */
export function installFakeBrowser(): FakeBrowser {
  const document = new FakeDocument();
  const chromeMeta = document.head.appendChild(document.createElement("meta"));
  chromeMeta.setAttribute("name", "theme-color");
  chromeMeta.setAttribute("content", "first-paint");
  const computedStyle = new Map<string, (documentElement: FakeElement) => string>();
  const storage = new Map<string, string>();
  const storageWrites: Array<[string, string]> = [];
  const windowListeners = new Map<string, Set<Listener>>();
  const window = {
    addEventListener(type: string, listener: Listener | null) {
      if (listener === null) return;
      const listeners = windowListeners.get(type) ?? new Set<Listener>();
      windowListeners.set(type, listeners);
      listeners.add(listener);
    },
    document,
    event: undefined,
    HTMLIFrameElement: FakeIFrameElement,
    getComputedStyle(element: unknown) {
      if (element !== document.documentElement) throw new Error("fake-dom computes styles for the document element only");
      return {
        getPropertyValue: (name: string) => computedStyle.get(name)?.(document.documentElement) ?? "",
      };
    },
    localStorage: {
      getItem: (key: string) => storage.get(key) ?? null,
      removeItem(key: string) {
        storage.delete(key);
      },
      setItem(key: string, value: string) {
        storage.set(key, value);
        storageWrites.push([key, value]);
      },
    },
    removeEventListener(type: string, listener: Listener | null) {
      if (listener === null) return;
      windowListeners.get(type)?.delete(listener);
    },
  };
  const globals = globalThis as { document?: unknown; IS_REACT_ACT_ENVIRONMENT?: boolean; window?: unknown };
  const previous = { document: globals.document, react: globals.IS_REACT_ACT_ENVIRONMENT, window: globals.window };
  globals.window = window;
  globals.document = document;
  globals.IS_REACT_ACT_ENVIRONMENT = true;
  return {
    chromeMeta,
    computedStyle,
    dispatchWindowEvent(type) {
      const dispatched = fakeEvent(type);
      for (const listener of Array.from(windowListeners.get(type) ?? [])) listener(dispatched);
      return dispatched;
    },
    document,
    storage,
    storageWrites,
    uninstall() {
      globals.window = previous.window;
      globals.document = previous.document;
      globals.IS_REACT_ACT_ENVIRONMENT = previous.react;
    },
    windowListeners,
  };
}
