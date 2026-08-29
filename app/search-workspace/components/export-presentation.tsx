import { AppIcon, StatusIcon, type AppIconName, type StatusIconTone } from "../../_components/app-icon";

interface ExportArtifactStatusProps {
  expired: boolean;
  expiry: string | null;
  metadataValid: boolean;
  titleId: string;
}

interface ExportStatusPresentation {
  detail: string;
  icon: AppIconName;
  title: string;
  tone: StatusIconTone;
}

function getExportStatusPresentation({
  expired,
  expiry,
  metadataValid,
}: Omit<ExportArtifactStatusProps, "titleId">): ExportStatusPresentation {
  if (!metadataValid) {
    return {
      detail: "The server returned incomplete artifact metadata. Create a new export to try again.",
      icon: "circle-x",
      title: "Export artifact unavailable",
      tone: "error",
    };
  }
  if (expired) {
    return {
      detail: `This artifact expired ${expiry ?? "at an unavailable time"}. Create a new export to download fresh results.`,
      icon: "hourglass",
      title: "Export artifact expired",
      tone: "warning",
    };
  }
  return {
    detail: "The connected server finished materializing this artifact.",
    icon: "check",
    title: "Export ready",
    tone: "success",
  };
}

export function getExportStatusTone(props: Omit<ExportArtifactStatusProps, "titleId">): StatusIconTone {
  return getExportStatusPresentation(props).tone;
}

export function ExportArtifactStatus(props: ExportArtifactStatusProps) {
  const presentation = getExportStatusPresentation(props);
  return (
    <header>
      <StatusIcon tone={presentation.tone} icon={presentation.icon} />
      <div>
        <h3 id={props.titleId}>{presentation.title}</h3>
        <p>{presentation.detail}</p>
      </div>
    </header>
  );
}

export function ExportDownloadContent({ format, pending }: { format: "CSV" | "JSON Lines"; pending: boolean }) {
  return (
    <>
      <AppIcon name={pending ? "loading" : "download"} size="sm" spin={pending} />
      {pending ? "Downloading…" : `Download ${format}`}
    </>
  );
}
