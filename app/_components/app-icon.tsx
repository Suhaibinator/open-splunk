import type { ComponentPropsWithoutRef } from "react";
import {
  Activity,
  Bell,
  ChartNoAxesCombined,
  ChartBar,
  ChartColumn,
  Check,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  ChevronUp,
  CircleAlert,
  CircleX,
  Clock,
  Copy,
  Database,
  Download,
  ExternalLink,
  FileText,
  FolderOpen,
  History,
  Hourglass,
  House,
  Info,
  LayoutDashboard,
  LoaderCircle,
  LogOut,
  List,
  Menu,
  Minus,
  MoreHorizontal,
  Pencil,
  Plus,
  RefreshCw,
  Save,
  Search,
  Settings,
  Share2,
  Square,
  TextWrap,
  Trash2,
  TriangleAlert,
  Users,
  X,
  Zap,
  type LucideIcon,
} from "lucide-react";

import { statusClassName, type StatusTone } from "./status";

const ICONS = {
  activity: Activity,
  alert: Bell,
  analytics: ChartNoAxesCombined,
  "bar-chart": ChartBar,
  "column-chart": ChartColumn,
  check: Check,
  "chevron-down": ChevronDown,
  "chevron-left": ChevronLeft,
  "chevron-right": ChevronRight,
  "chevron-up": ChevronUp,
  "circle-alert": CircleAlert,
  "circle-x": CircleX,
  clock: Clock,
  copy: Copy,
  database: Database,
  download: Download,
  "external-link": ExternalLink,
  file: FileText,
  open: FolderOpen,
  history: History,
  hourglass: Hourglass,
  home: House,
  info: Info,
  dashboard: LayoutDashboard,
  loading: LoaderCircle,
  logout: LogOut,
  list: List,
  menu: Menu,
  minus: Minus,
  more: MoreHorizontal,
  edit: Pencil,
  plus: Plus,
  refresh: RefreshCw,
  save: Save,
  search: Search,
  settings: Settings,
  share: Share2,
  stop: Square,
  wrap: TextWrap,
  trash: Trash2,
  warning: TriangleAlert,
  users: Users,
  close: X,
  mode: Zap,
} satisfies Record<string, LucideIcon>;

export type AppIconName = keyof typeof ICONS;
export type AppIconSize = "xs" | "sm" | "md" | "lg";

export interface AppIconProps extends Omit<ComponentPropsWithoutRef<"svg">, "children" | "name"> {
  name: AppIconName;
  size?: AppIconSize;
  spin?: boolean;
}

export function AppIcon({ className, name, size = "sm", spin = false, ...props }: AppIconProps) {
  const Icon = ICONS[name];
  return (
    <Icon
      {...props}
      aria-hidden="true"
      className={["app-icon", `app-icon--${size}`, spin ? "app-icon--spin" : "", className ?? ""].filter(Boolean).join(" ")}
      focusable="false"
      strokeWidth={2}
    />
  );
}

/**
 * The tone a status icon may carry.
 *
 * A narrowing of `StatusTone`: an icon is a snapshot, so it has no `running`
 * spelling of its own -- a job that is still going renders `info` with a
 * spinning glyph, which is motion the caller already asks for through `spin`.
 */
export type StatusIconTone = Exclude<StatusTone, "running">;

export interface StatusIconProps {
  icon: AppIconName;
  tone: StatusIconTone;
  spin?: boolean;
}

/** A tone-filled disc carrying one glyph, from the shared `.status` family. */
export function StatusIcon({ icon, spin = false, tone }: StatusIconProps) {
  return (
    <span className={statusClassName("icon", tone)} aria-hidden="true">
      <AppIcon name={icon} size="xs" spin={spin} />
    </span>
  );
}
