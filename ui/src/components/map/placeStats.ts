import type { Geometry, MultiPolygon, Polygon, Position } from "geojson";
import { getLevelLabel, type DistrictCollection, type MunicipalityFeature } from "@/lib/geo";

// Facts about a place worked out from the boundary data itself (area,
// neighbours, local governments), so the info card never shows made-up numbers.

const EARTH_RADIUS_M = 6378137;
const rad = (deg: number) => (deg * Math.PI) / 180;

function polygonsOf(geometry: Geometry): Position[][][] {
  if (geometry.type === "Polygon") return [(geometry as Polygon).coordinates];
  if (geometry.type === "MultiPolygon") return (geometry as MultiPolygon).coordinates;
  return [];
}

// Area of a ring on the sphere (the method used by Mapbox's geojson-area).
function ringArea(ring: Position[]): number {
  let total = 0;
  for (let i = 0; i < ring.length; i++) {
    const [lng1, lat1] = ring[i];
    const [lng2, lat2] = ring[(i + 1) % ring.length];
    total += (rad(lng2) - rad(lng1)) * (2 + Math.sin(rad(lat1)) + Math.sin(rad(lat2)));
  }
  return Math.abs((total * EARTH_RADIUS_M * EARTH_RADIUS_M) / 2);
}

/** Approximate area in km², measured from the boundary shape. */
export function areaKm2(geometries: Geometry[]): number {
  let squareMetres = 0;
  for (const geometry of geometries) {
    for (const [outer, ...holes] of polygonsOf(geometry)) {
      squareMetres += ringArea(outer);
      for (const hole of holes) squareMetres -= ringArea(hole);
    }
  }
  return squareMetres / 1e6;
}

// Two districts are neighbours when their borders share points. The shapes come
// from one dataset, so shared borders line up to within a few metres.
const CELL = 0.01; // degrees, about 1 km
const TOLERANCE = 0.002; // degrees, about 200 m

function vertices(geometry: Geometry): Position[] {
  return polygonsOf(geometry).flat(2);
}

const neighbourCache = new WeakMap<DistrictCollection, Map<string, string[]>>();

/** Districts sharing a border with `name`, sorted by name. */
export function findNeighbours(districts: DistrictCollection, name: string): string[] {
  let cache = neighbourCache.get(districts);
  if (!cache) neighbourCache.set(districts, (cache = new Map()));
  const cached = cache.get(name);
  if (cached) return cached;

  const target = districts.features.find((f) => f.properties.DISTRICT === name);
  if (!target) return [];

  const grid = new Map<string, Position[]>();
  for (const p of vertices(target.geometry)) {
    const key = `${Math.floor(p[0] / CELL)},${Math.floor(p[1] / CELL)}`;
    const bucket = grid.get(key);
    if (bucket) bucket.push(p);
    else grid.set(key, [p]);
  }
  function touches(p: Position) {
    const cx = Math.floor(p[0] / CELL);
    const cy = Math.floor(p[1] / CELL);
    for (let dx = -1; dx <= 1; dx++) {
      for (let dy = -1; dy <= 1; dy++) {
        for (const q of grid.get(`${cx + dx},${cy + dy}`) ?? []) {
          if (Math.abs(q[0] - p[0]) < TOLERANCE && Math.abs(q[1] - p[1]) < TOLERANCE) return true;
        }
      }
    }
    return false;
  }

  const result: string[] = [];
  for (const other of districts.features) {
    if (other === target) continue;
    // Two shared points rather than one, so districts meeting at a single
    // corner don't count.
    let shared = 0;
    for (const p of vertices(other.geometry)) {
      if (touches(p) && ++shared >= 2) break;
    }
    if (shared >= 2) result.push(other.properties.DISTRICT);
  }
  result.sort();
  cache.set(name, result);
  return result;
}

const GOVERNMENT_LEVELS = ["Metropolitan city", "Sub-metropolitan city", "Municipality", "Rural municipality"];
const PLURALS: Record<string, string> = {
  "Metropolitan city": "metropolitan cities",
  "Sub-metropolitan city": "sub-metropolitan cities",
  Municipality: "municipalities",
  "Rural municipality": "rural municipalities",
};

export interface LocalGovernmentSummary {
  total: number;
  /** e.g. ["1 metropolitan city", "4 rural municipalities"] */
  breakdown: string[];
  /** National parks, reserves and similar areas, which aren't local governments. */
  protectedAreas: number;
}

export function summariseLocalGovernments(features: MunicipalityFeature[]): LocalGovernmentSummary {
  const seen = new Set<string>();
  const counts = new Map<string, number>();
  let protectedAreas = 0;
  for (const f of features) {
    if (seen.has(f.properties.N_ID)) continue; // multi-part places appear more than once
    seen.add(f.properties.N_ID);
    const label = getLevelLabel(f.properties.LEVEL);
    if (GOVERNMENT_LEVELS.includes(label)) counts.set(label, (counts.get(label) ?? 0) + 1);
    else protectedAreas += 1;
  }
  const breakdown = GOVERNMENT_LEVELS.filter((level) => counts.has(level)).map((level) => {
    const n = counts.get(level)!;
    return `${n} ${n === 1 ? level.toLowerCase() : PLURALS[level]}`;
  });
  return {
    total: [...counts.values()].reduce((a, b) => a + b, 0),
    breakdown,
    protectedAreas,
  };
}
