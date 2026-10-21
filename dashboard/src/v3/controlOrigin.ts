export function localOwnerControlUrl(href: string, enabled: boolean): string | null {
  const url = new URL(href);
  if (!enabled || url.protocol !== "http:" || url.hostname !== "localhost") return null;
  url.hostname = "127.0.0.1";
  return url.href;
}
