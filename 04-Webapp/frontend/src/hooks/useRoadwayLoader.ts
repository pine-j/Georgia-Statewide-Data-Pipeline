import { startTransition, useEffect, useRef, useState } from "react";

import { getRoadwayManifest, getRoadways } from "../services/api";
import { RoadwayFeatureCollection, RoadwayManifest } from "../types/api";

const DEFAULT_CHUNK_SIZE = 10000;
const MAX_CONCURRENT_REQUESTS = 4;

export interface RoadwayLoaderState {
  bounds: [number, number, number, number] | null;
  error: Error | null;
  etaSeconds: number | null;
  isLoading: boolean;
  isManifestLoading: boolean;
  loadedSegments: number;
  loadToken: number;
  manifest: RoadwayManifest | null;
  progressPercent: number;
  roadwayChunks: RoadwayFeatureCollection[];
  totalSegments: number;
}

function createEmptyState(loadToken: number): RoadwayLoaderState {
  return {
    bounds: null,
    error: null,
    etaSeconds: null,
    isLoading: false,
    isManifestLoading: false,
    loadedSegments: 0,
    loadToken,
    manifest: null,
    progressPercent: 0,
    roadwayChunks: [],
    totalSegments: 0,
  };
}

function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}

export function useRoadwayLoader(
  district: number | null,
  counties: string[],
  enabled: boolean,
): RoadwayLoaderState {
  const [state, setState] = useState<RoadwayLoaderState>(() => createEmptyState(0));
  const loadTokenRef = useRef(0);
  const countiesKey = counties.join("|");

  useEffect(() => {
    const controller = new AbortController();
    loadTokenRef.current += 1;
    const nextLoadToken = loadTokenRef.current;

    if (!enabled) {
      startTransition(() => {
        setState(createEmptyState(nextLoadToken));
      });

      return () => {
        controller.abort();
      };
    }

    startTransition(() => {
      setState({
        ...createEmptyState(nextLoadToken),
        isLoading: true,
        isManifestLoading: true,
      });
    });

    void (async () => {
      try {
        const manifest = await getRoadwayManifest(
          { district, counties },
          DEFAULT_CHUNK_SIZE,
          controller.signal,
        );

        if (controller.signal.aborted) {
          return;
        }

        startTransition(() => {
          setState((current) => ({
            ...current,
            bounds: manifest.bounds,
            isManifestLoading: false,
            manifest,
            totalSegments: manifest.total_segments,
          }));
        });

        if (manifest.total_segments === 0) {
          startTransition(() => {
            setState((current) => ({
              ...current,
              isLoading: false,
            }));
          });
          return;
        }

        const startedAt = performance.now();
        let loadedSegments = 0;
        const chunkOffsets = Array.from(
          { length: manifest.chunk_count },
          (_, index) => index * manifest.chunk_size,
        );
        let nextChunkIndex = 0;

        const loadNextChunk = async () => {
          while (!controller.signal.aborted) {
            const currentIndex = nextChunkIndex;
            if (currentIndex >= chunkOffsets.length) {
              return;
            }

            nextChunkIndex += 1;
            const offset = chunkOffsets[currentIndex];
            const chunk = await getRoadways(
              { district, counties },
              manifest.chunk_size,
              offset,
              controller.signal,
            );

            if (controller.signal.aborted) {
              return;
            }

            loadedSegments += chunk.features.length;
            const elapsedSeconds = Math.max((performance.now() - startedAt) / 1000, 0.001);
            const segmentsPerSecond = loadedSegments / elapsedSeconds;
            const remainingSegments = Math.max(manifest.total_segments - loadedSegments, 0);
            const etaSeconds =
              remainingSegments > 0 && segmentsPerSecond > 0
                ? remainingSegments / segmentsPerSecond
                : null;

            startTransition(() => {
              setState((current) => ({
                ...current,
                etaSeconds,
                isLoading: loadedSegments < manifest.total_segments,
                loadedSegments,
                progressPercent: (loadedSegments / manifest.total_segments) * 100,
                roadwayChunks: [...current.roadwayChunks, chunk],
              }));
            });
          }
        };

        const workerCount = Math.min(MAX_CONCURRENT_REQUESTS, chunkOffsets.length);
        await Promise.all(Array.from({ length: workerCount }, () => loadNextChunk()));
      } catch (error) {
        if (isAbortError(error)) {
          return;
        }

        startTransition(() => {
          setState((current) => ({
            ...current,
            error: error instanceof Error ? error : new Error("Roadway loading failed."),
            isLoading: false,
            isManifestLoading: false,
          }));
        });
      }
    })();

    return () => {
      controller.abort();
    };
  }, [countiesKey, district, enabled]);

  return state;
}
