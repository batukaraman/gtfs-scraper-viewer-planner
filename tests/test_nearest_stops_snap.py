"""BallTree snap index vs brute-force nearest_stops (same haversine)."""

import unittest

from planner.preprocess import (
    RaptorContext,
    _build_snap_index,
    _exact_haversine_m,
    nearest_stops,
)


def _brute_nearest(
    stop_coords: dict,
    lat: float,
    lon: float,
    *,
    max_m: float,
    k: int,
) -> list[tuple[str, float]]:
    cand: list[tuple[str, float]] = []
    for sid, (slat, slon) in stop_coords.items():
        d = _exact_haversine_m(lat, lon, slat, slon)
        if d <= max_m:
            cand.append((sid, d))
    cand.sort(key=lambda x: (x[1], x[0]))
    return cand[:k]


def _minimal_ctx(stop_coords: dict, *, with_index: bool) -> RaptorContext:
    tree, ids = (None, None)
    if with_index:
        tree, ids = _build_snap_index(stop_coords)
    return RaptorContext(
        footpaths={},
        route_ids=[],
        route_stops={},
        trips_by_route={},
        trips_by_trip_id={},
        board_at={},
        stop_to_routes={},
        stop_coords=stop_coords,
        route_meta={},
        shape_by_trip={},
        all_stops=set(stop_coords),
        stop_names={sid: sid for sid in stop_coords},
        snap_ball_tree=tree,
        snap_stop_ids=ids,
    )


class TestNearestStopsSnap(unittest.TestCase):
    def setUp(self) -> None:
        self.coords = {
            f"s{i}": (41.0 + (i % 20) * 0.002, 29.0 + (i // 20) * 0.002) for i in range(400)
        }

    def test_index_matches_brute_random_queries(self) -> None:
        ctx = _minimal_ctx(self.coords, with_index=True)
        queries = [
            (41.015, 29.01, 350.0, 6),
            (41.0, 29.0, 120.0, 3),
            (41.05, 29.05, 900.0, 10),
        ]
        for lat, lon, max_m, k in queries:
            with self.subTest(lat=lat, lon=lon, max_m=max_m, k=k):
                got = nearest_stops(ctx, lat, lon, max_m=max_m, k=k)
                want = _brute_nearest(self.coords, lat, lon, max_m=max_m, k=k)
                self.assertEqual(got, want)

    def test_no_index_same_as_brute(self) -> None:
        ctx = _minimal_ctx(self.coords, with_index=False)
        got = nearest_stops(ctx, 41.012, 29.008, max_m=500.0, k=8)
        want = _brute_nearest(self.coords, 41.012, 29.008, max_m=500.0, k=8)
        self.assertEqual(got, want)


if __name__ == "__main__":
    unittest.main()
