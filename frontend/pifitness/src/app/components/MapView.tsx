"use client";

import { useEffect, useRef } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

interface MapViewProps {
  /** Optional center coordinates [lng, lat] */
  center?: [number, number];
  /** Optional initial zoom level */
  zoom?: number;
  /** Optional CSS class name for the container */
  className?: string;
  /** Optional container height (default: "400px") */
  height?: string;
}

/**
 * Basic MapLibre GL map component.
 *
 * Initializes a WebGL-powered map with a tile layer. Automatically
 * cleans up on unmount.
 *
 * Usage:
 *   <MapView center={[-122.4194, 37.7749]} zoom={12} />
 */
export default function MapView({
  center = [-122.4194, 37.7749],
  zoom = 10,
  className = "",
  height = "400px",
}: MapViewProps) {
  const mapContainerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);

  useEffect(() => {
    if (mapRef.current || !mapContainerRef.current) return;

    const map = new maplibregl.Map({
      container: mapContainerRef.current,
      style: "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
      center,
      zoom,
    });

    // Add navigation controls (zoom +/-)
    map.addControl(new maplibregl.NavigationControl(), "top-right");

    mapRef.current = map;

    // Cleanup on unmount
    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, [center, zoom]);

  return (
    <div
      ref={mapContainerRef}
      className={`w-full rounded-lg overflow-hidden ${className}`}
      style={{ height }}
    />
  );
}