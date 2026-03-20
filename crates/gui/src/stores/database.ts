import { writable } from "svelte/store";
import type { DbInfo, TablesResponse, Page } from "$lib/types";

export const dbInfo = writable<DbInfo | null>(null);
export const tables = writable<TablesResponse | null>(null);
export const currentPage = writable<Page>("query");
export const statusMessage = writable<string>("");
export const errorMessage = writable<string>("");
