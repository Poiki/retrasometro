import { readFileSync } from "node:fs";
import { join } from "node:path";

type Point = [number, number];
type Bounds = number[][];

const samePoint = (a: Point, b: Point) => Math.abs(a[0] - b[0]) < 1e-9 && Math.abs(a[1] - b[1]) < 1e-9;

export const clipRailwayLine = (line: Point[], bounds: Bounds): Point[][] => {
  const [[south, west], [north, east]] = bounds as [Point, Point];
  const result: Point[][] = [];
  let current: Point[] = [];
  for (let i = 1; i < line.length; i++) {
    const a = line[i - 1]!;
    const b = line[i]!;
    const dx = b[0] - a[0], dy = b[1] - a[1];
    let start = 0, end = 1;
    for (const [p, q] of [[-dx, a[0] - west], [dx, east - a[0]], [-dy, a[1] - south], [dy, north - a[1]]] as const) {
      if (p === 0) {
        if (q < 0) { end = -1; break; }
      } else if (p < 0) start = Math.max(start, q / p);
      else end = Math.min(end, q / p);
    }
    if (start > end) {
      if (current.length > 1) result.push(current);
      current = [];
      continue;
    }
    const first: Point = start === 0 ? a : [a[0] + start * dx, a[1] + start * dy];
    const last: Point = end === 1 ? b : [a[0] + end * dx, a[1] + end * dy];
    if (samePoint(first, last)) continue;
    if (current.length && !samePoint(current[current.length - 1]!, first)) {
      if (current.length > 1) result.push(current);
      current = [];
    }
    if (!current.length) current.push(first);
    current.push(last);
  }
  if (current.length > 1) result.push(current);
  return result;
};

let railways: Array<{ line: Point[]; south: number; west: number; north: number; east: number }> | null = null;

export const getRailwaySegments = (bounds: Bounds) => {
  if (!railways) {
    const data = JSON.parse(readFileSync(join(import.meta.dir, "data", "railways.geojson"), "utf8"));
    if (data.geometry?.type !== "MultiLineString" || !Array.isArray(data.geometry.coordinates)) throw new Error("Invalid railway geometry");
    railways = (data.geometry.coordinates as Point[][]).map((line) => {
      let south = Infinity, west = Infinity, north = -Infinity, east = -Infinity;
      for (const [lon, lat] of line) {
        if (!Number.isFinite(lon) || !Number.isFinite(lat) || Math.abs(lon) > 180 || Math.abs(lat) > 85.05112878) throw new Error("Invalid railway coordinate");
        south = Math.min(south, lat); north = Math.max(north, lat);
        west = Math.min(west, lon); east = Math.max(east, lon);
      }
      return { line, south, west, north, east };
    });
  }
  const [[south, west], [north, east]] = bounds as [Point, Point];
  const segments: Point[][] = [];
  for (const railway of railways) {
    if (railway.north < south || railway.south > north || railway.east < west || railway.west > east) continue;
    for (const line of clipRailwayLine(railway.line, bounds)) segments.push(line.map(([lon, lat]) => [lat, lon]));
  }
  return segments;
};
