import polars as pl


class Batters:
    def __init__(self, filepath: str):
        self.stats_df: pl.LazyFrame = (
            pl.scan_csv(filepath + "/players_career_batting_stats.csv")
            # Filtering the dataframe for level_id = 1 and split_id = 1 (this means MLB stats and all pa not just for left or right handers)
            .filter((pl.col("level_id") == 1) & (pl.col("split_id") == 1))
        )

    def calculate_rates(self) -> pl.LazyFrame:
        return (
            self.stats_df.group_by("player_id")
            .agg(
                [
                    pl.col("pa").sum().alias("pa"),
                    pl.col("bb").sum().alias("bb"),
                    pl.col("k").sum().alias("k"),
                    pl.col("h").sum().alias("h"),
                    pl.col("d").sum().alias("d"),
                    pl.col("t").sum().alias("t"),
                    pl.col("hr").sum().alias("hr"),
                    pl.col("hp").sum().alias("hp"),
                    pl.col("pitches_seen").sum().alias("pitches_seen"),
                ]
            )
            .with_columns(
                [
                    (pl.col("bb") / pl.col("pa")).alias("bb%_mlb"),
                    (pl.col("k") / pl.col("pa")).alias("k%_mlb"),
                    (pl.col("h") / pl.col("pa")).alias("1b%_mlb"),
                    (pl.col("d") / pl.col("pa")).alias("2b%_mlb"),
                    (pl.col("t") / pl.col("pa")).alias("3b%_mlb"),
                    (pl.col("hr") / pl.col("pa")).alias("hr%_mlb"),
                    # calculate MLB rate stats (hp = hit by pitch)
                    (pl.col("hp") / pl.col("pa")).alias("hp%_mlb"),
                    # rename pa to pa_mlb (to prevent confusion with current single-season pa, which is just called pa further down)
                    (pl.col("pitches_seen") / pl.col("pa")).alias(
                        "pitches/plate_appearance_mlb"
                    ),
                ]
            )
            .rename({"pa": "pa_mlb"})
            .with_columns([pl.all().round(3)])
            .select([
    "player_id",
    "pa_mlb",
    "bb%_mlb",
    "k%_mlb",
    "1b%_mlb",
    "2b%_mlb",
    "3b%_mlb",
    "hr%_mlb",
    "hp%_mlb",
    "pitches/plate_appearance_mlb",
])
        )

    def calculate_yearly_sum(self) -> pl.LazyFrame:
        # Filter to get the latest year only and where split_id and level_id are both 1 (this means
        # MLB stats and all pa not just for left or right handers)
        stats_cols = [
            "ab",
            "h",
            "k",
            "pa",
            "pitches_seen",
            "g",
            "gs",
            "d",
            "t",
            "hr",
            "r",
            "rbi",
            "sb",
            "cs",
            "bb",
            "ibb",
            "gdp",
            "sh",
            "sf",
            "hp",
            "ci",
            "wpa",
            "stint",
            "ubr",
            "war",
        ]

        return (
            self.stats_df.filter(pl.col("year") == pl.col("year").max())
            .group_by("player_id")
            .agg(pl.col(stats_cols).sum())
        )
