"use client";

import {
  Children,
  Fragment,
  isValidElement,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
  type ComponentPropsWithoutRef,
  type KeyboardEvent,
  type ReactNode,
} from "react";

import { AppIcon } from "./app-icon";

type SelectValue = string;

export interface SelectOptionProps {
  children: ReactNode;
  disabled?: boolean;
  value?: SelectValue;
}

/**
 * A data-only child for Select. Keeping options declarative makes maps and
 * fragments work naturally without introducing a second, hidden select tree.
 */
export function SelectOption(props: SelectOptionProps) {
  void props;
  return null;
}

interface OptionData {
  disabled: boolean;
  label: string;
  value: string;
}

function optionLabel(children: ReactNode): string {
  if (typeof children === "string" || typeof children === "number") return String(children);
  return Children.toArray(children).map((child) => {
    if (typeof child === "string" || typeof child === "number") return String(child);
    return "";
  }).join("");
}

function optionsFromChildren(children: ReactNode): OptionData[] {
  const options: OptionData[] = [];
  const visit = (nodes: ReactNode) => {
    Children.forEach(nodes, (node) => {
      if (!isValidElement<SelectOptionProps>(node)) return;
      if (node.type === Fragment) {
        visit(node.props.children);
        return;
      }
      if (node.type !== SelectOption) return;
      const label = optionLabel(node.props.children);
      options.push({
        disabled: node.props.disabled ?? false,
        label,
        value: String(node.props.value ?? label),
      });
    });
  };
  visit(children);
  return options;
}

function nextEnabledIndex(options: OptionData[], from: number, direction: 1 | -1): number {
  if (options.length === 0) return -1;
  for (let offset = 1; offset <= options.length; offset += 1) {
    const index = (from + direction * offset + options.length) % options.length;
    if (!options[index]?.disabled) return index;
  }
  return -1;
}

export interface SelectProps extends Omit<ComponentPropsWithoutRef<"button">, "children" | "defaultValue" | "onChange" | "value"> {
  children: ReactNode;
  defaultValue?: SelectValue;
  name?: string;
  onValueChange?: (value: string) => void;
  placeholder?: string;
  required?: boolean;
  value?: SelectValue;
}

/**
 * The shared, themeable single-value combobox. It deliberately uses buttons,
 * a listbox, and a validation input rather than a native select so all browsers
 * receive the same themed surface while ordinary forms keep their semantics.
 */
export function Select({
  children,
  className,
  defaultValue,
  disabled = false,
  id,
  name,
  onBlur,
  onValueChange,
  placeholder = "Select…",
  required = false,
  value,
  ...buttonProps
}: SelectProps) {
  const options = useMemo(() => optionsFromChildren(children), [children]);
  const isControlled = value !== undefined;
  const initialValue = String(defaultValue ?? options.find((option) => !option.disabled)?.value ?? "");
  const [uncontrolledValue, setUncontrolledValue] = useState(initialValue);
  const firstEnabledOption = options.find((option) => !option.disabled);
  const requestedValue = isControlled ? String(value) : uncontrolledValue;
  const requestedIndex = options.findIndex((option) => option.value === requestedValue);
  const selectedValue = !isControlled && requestedIndex < 0 ? firstEnabledOption?.value ?? "" : requestedValue;
  const selectedIndex = options.findIndex((option) => option.value === selectedValue);
  const selectedOption = options[selectedIndex];
  const generatedId = useId().replace(/:/gu, "");
  const triggerId = id ?? `select-${generatedId}`;
  const listboxId = `${triggerId}-listbox`;
  const inputRef = useRef<HTMLInputElement>(null);
  const listboxRef = useRef<HTMLDivElement>(null);
  const rootRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const activeOptionValueRef = useRef(options[selectedIndex]?.value);
  const typeaheadRef = useRef({ text: "", timeout: 0 });
  const [activeIndex, setActiveIndex] = useState(selectedIndex);
  const [invalid, setInvalid] = useState(false);
  const [open, setOpen] = useState(false);
  const selectDisabled = disabled || firstEnabledOption === undefined;
  const ariaInvalid = buttonProps["aria-invalid"] === true || buttonProps["aria-invalid"] === "true" || invalid;

  const close = (returnFocus = false) => {
    setOpen(false);
    if (returnFocus) requestAnimationFrame(() => triggerRef.current?.focus());
  };

  const activate = (index: number) => {
    activeOptionValueRef.current = options[index]?.value;
    setActiveIndex(index);
  };

  const commit = (index: number, fromPointer = false) => {
    const option = options[index];
    if (!option || option.disabled) return;
    if (!fromPointer && option.value !== activeOptionValueRef.current) {
      close();
      return;
    }
    if (!isControlled) setUncontrolledValue(option.value);
    setInvalid(false);
    onValueChange?.(option.value);
    close(true);
  };

  const openListbox = (index = selectedIndex) => {
    if (selectDisabled) return;
    const next = index >= 0 && !options[index]?.disabled ? index : nextEnabledIndex(options, -1, 1);
    activate(next);
    setOpen(true);
  };

  useEffect(() => {
    const listbox = listboxRef.current;
    if (!listbox) return;
    if (open) {
      listbox.showPopover?.();
      const focusFrame = requestAnimationFrame(() => triggerRef.current?.focus());
      return () => window.cancelAnimationFrame(focusFrame);
    }
    else if (listbox.matches(":popover-open")) listbox.hidePopover?.();
  }, [open]);

  useEffect(() => {
    if (buttonProps["aria-label"] !== undefined || buttonProps["aria-labelledby"] !== undefined) return;
    const label = triggerRef.current?.closest("label");
    const labelText = label?.querySelector(":scope > span")?.textContent?.trim();
    if (labelText) triggerRef.current?.setAttribute("aria-label", labelText);
  }, [buttonProps]);

  useEffect(() => {
    const form = inputRef.current?.form;
    if (!form || isControlled) return;
    const reset = () => queueMicrotask(() => setUncontrolledValue(initialValue));
    form.addEventListener("reset", reset);
    return () => form.removeEventListener("reset", reset);
  }, [initialValue, isControlled]);

  useEffect(() => {
    const dismiss = (event: PointerEvent) => {
      if (rootRef.current?.contains(event.target as Node)) return;
      setOpen(false);
    };
    document.addEventListener("pointerdown", dismiss);
    return () => document.removeEventListener("pointerdown", dismiss);
  }, []);

  useEffect(() => {
    if (!open) return;
    const dismissEscape = (event: globalThis.KeyboardEvent) => {
      if (event.key !== "Escape") return;
      event.preventDefault();
      event.stopImmediatePropagation();
      event.stopPropagation();
      setOpen(false);
      requestAnimationFrame(() => triggerRef.current?.focus());
    };
    document.addEventListener("keydown", dismissEscape, true);
    return () => document.removeEventListener("keydown", dismissEscape, true);
  }, [open]);

  useEffect(() => {
    if (!open || activeIndex < 0) return;
    const option = listboxRef.current?.querySelector<HTMLElement>(`[data-option-index="${activeIndex}"]`);
    option?.scrollIntoView({ block: "nearest" });
  }, [activeIndex, open]);

  const onKeyDown = (event: KeyboardEvent<HTMLButtonElement>) => {
    if (selectDisabled) return;
    const first = options.findIndex((option) => !option.disabled);
    const last = options.findLastIndex((option) => !option.disabled);
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      const direction = event.key === "ArrowDown" ? 1 : -1;
      const base = open ? activeIndex : selectedIndex;
      const next = nextEnabledIndex(options, base, direction);
      if (!open) openListbox(next);
      else activate(next);
      return;
    }
    if (event.key === "Home" || event.key === "End") {
      event.preventDefault();
      if (!open) openListbox(event.key === "Home" ? first : last);
      else activate(event.key === "Home" ? first : last);
      return;
    }
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      if (open) commit(activeIndex);
      else openListbox();
      return;
    }
    if (event.key === "Escape") {
      if (!open) return;
      event.preventDefault();
      event.stopPropagation();
      close(true);
      return;
    }
    if (event.key === "Tab") {
      close();
      return;
    }
    if (event.key.length !== 1) return;
    window.clearTimeout(typeaheadRef.current.timeout);
    typeaheadRef.current.timeout = window.setTimeout(() => { typeaheadRef.current.text = ""; }, 500);
    typeaheadRef.current.text += event.key.toLocaleLowerCase();
    const match = options.findIndex((option) => !option.disabled && option.label.toLocaleLowerCase().startsWith(typeaheadRef.current.text));
    if (match >= 0) {
      event.preventDefault();
      if (!open) openListbox(match);
      else activate(match);
    }
  };

  const triggerClassName = ["select__trigger", className].filter(Boolean).join(" ");
  const optionId = activeIndex >= 0 ? `${listboxId}-option-${activeIndex}` : undefined;
  return (
    <div className="select" ref={rootRef}>
      <button
        {...buttonProps}
        aria-activedescendant={open ? optionId : undefined}
        aria-controls={listboxId}
        aria-expanded={open}
        aria-haspopup="listbox"
        aria-invalid={ariaInvalid || undefined}
        aria-required={required || undefined}
        className={triggerClassName}
        disabled={selectDisabled}
        id={triggerId}
        onBlur={onBlur}
        onClick={() => (open ? close() : openListbox())}
        onKeyDown={onKeyDown}
        ref={triggerRef}
        role="combobox"
        type="button"
      >
        <span className="select__value">{selectedOption?.label ?? placeholder}</span>
        <AppIcon name="chevron-down" size="xs" />
      </button>
      <input
        aria-hidden="true"
        className="select__input"
        disabled={selectDisabled}
        name={name}
        onChange={() => {}}
        onInvalid={(event) => {
          event.preventDefault();
          setInvalid(true);
          triggerRef.current?.focus();
        }}
        ref={inputRef}
        required={required}
        tabIndex={-1}
        type="text"
        value={selectedValue}
      />
      <div
        aria-labelledby={triggerId}
        className="select__listbox"
        data-open={open}
        id={listboxId}
        popover="manual"
        ref={listboxRef}
        role="listbox"
      >
        {options.length === 0 ? <span aria-selected="false" className="select__empty" role="option">No options available</span> : options.map((option, index) => (
          <button
            role="option"
            aria-selected={index === selectedIndex}
            className="select__option"
            data-active={index === activeIndex}
            data-option-index={index}
            disabled={option.disabled}
            id={`${listboxId}-option-${index}`}
            key={option.value}
            onClick={() => commit(index, true)}
            onPointerDown={(event) => event.preventDefault()}
            tabIndex={-1}
            type="button"
          >
            {option.label}
          </button>
        ))}
      </div>
    </div>
  );
}
