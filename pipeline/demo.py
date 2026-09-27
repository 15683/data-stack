"""Small deterministic synthetic source. Never used as a fallback for the live API."""

import copy


def snapshot(revision=1):
    teams = [(900001, "Demo Owls"), (900002, "Demo Foxes"), (900003, "Demo Bears")]
    matches = []
    for index, (home, away, goals) in enumerate([(0, 1, (2, 0)), (1, 2, (1, 1)), (2, 0, (0, 3))]):
        matches.append(
            {
                "id": 990001 + index,
                "utcDate": f"2026-01-0{index + 1}T18:00:00Z",
                "status": "FINISHED",
                "matchday": index + 1,
                "stage": "LEAGUE_STAGE",
                "lastUpdated": "2026-01-04T00:00:00Z",
                "homeTeam": dict(zip(("id", "name"), teams[home], strict=True)),
                "awayTeam": dict(zip(("id", "name"), teams[away], strict=True)),
                "competition": {"code": "DEMO", "name": "Synthetic Demo League"},
                "season": {"id": 9000, "startDate": "2026-01-01", "endDate": "2026-12-31"},
                "score": {
                    "fullTime": {"home": goals[0], "away": goals[1]},
                    "halfTime": {"home": 0, "away": 0},
                },
            }
        )
    if revision == 2:
        matches[0]["score"]["fullTime"]["home"] = 4
        matches[0]["lastUpdated"] = "2026-01-05T00:00:00Z"
    return copy.deepcopy({"matches": matches, "resultSet": {"count": 3}})
