export function createErrorMessage(fallback: string): (error: unknown) => string {
  return (error) => error instanceof Error && error.message.trim().length > 0
    ? error.message
    : fallback;
}
