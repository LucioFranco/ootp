import polars as pl

class Pitchers:
    def __init__(self, filepath: str):
        self.stats_df: pl.LazyFrame = (
            pl.scan_csv(filepath + "/players_career_pitching_stats.csv")
            .filter((pl.col("level_id") == 1) & (pl.col("split_id") == 1))
        )

    def calculate_yearly_sum(self) -> pl.LazyFrame:
        sum_cols = ['ip', 'war', 'ra9war']

        return (
            self.stats_df.filter(pl.col("year") == pl.col("year").max())
            .group_by("player_id")
            .agg(pl.col(sum_cols).sum())
        )
