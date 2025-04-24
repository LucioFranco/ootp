import polars as pl

@pl.api.register_lazyframe_namespace("mops")
class War:
    def __init__(self, df: pl.LazyFrame):
        self.df: pl.LazyFrame = df

    def with_orc_per_game(self):
        self = (
            self.with_bb_percent()
            .with_k_percent()
            .with_hr_percent()
            .with_2b_percent()
            .with_3b_percent()
            .with_1b_percent()
        )

        # composite ORC per game using standardized rate differentials
        orc_expr = (
            ((pl.col("bb%") - 0.0738) / 0.875)
            + ((pl.col("k%") - 0.2195) / -1.217)
            + ((pl.col("hr%") - 0.0272) / 0.219)
            + ((pl.col("2b%") - 0.0628) / 0.693)
            + ((pl.col("3b%") - 0.0044) / 0.0519)
            + ((pl.col("1b%") - 0.28) / 0.594)
        ).alias("orc_per_game")
        self.df = self.df.with_columns(orc_expr)

        return self

    def with_toWAR(self) -> pl.LazyFrame:
        self = self.with_orc_per_game()

        to_war_expr = (pl.col("orc_per_game") * 162) / 10

        return self.df.with_columns(to_war_expr.alias("toWAR"))

    def with_bb_percent(self):
        eye = pl.col("batting_ratings_overall_eye")
        bb_expr = (
            pl.when(eye <= 100)
            .then(eye * 0.0007268758188 + 0.001460739)
            .otherwise(eye * 0.0012280964 - 0.0469974639)
        )
        self.df = self.df.with_columns(bb_expr.alias("bb%"))
        return self

    def with_k_percent(self):
        # calculate k% (with fudge factor to account for high avK players having too high a OPS+ projection vs career performance - this is based on OOTP 24 gameplay experience)
        # 0.1 is the fudge factor - this is applied here and in the potential calculations
        # strikeout rating also capped at 180 to prevent sky-high k% projections
        # before adding this players with high avK and gap power but low HR power were getting too high of an OPS+ projection
        sk = pl.col("batting_ratings_overall_strikeouts")
        strikeouts = sk.clip(upper_bound=180)
        adjusted = strikeouts + (100 - strikeouts) * 0.1

        k_expr = (
            pl.when(strikeouts <= 100)
            .then(adjusted * -0.002454367 + 0.4655792299)
            .when((strikeouts >= 101) & (strikeouts <= 220))
            .then(adjusted * -0.0016592514 + 0.383395059)
            .otherwise(0.02385)
        )
        self.df = self.df.with_columns(k_expr.alias("k%"))
        return self

    def with_hr_percent(self):
        power = pl.col("batting_ratings_overall_power")
        hr_expr = (
            pl.when(power <= 100)
            .then(power * 0.0001965717055 + 0.0057097943)
            .otherwise(power * 0.0005767110238 - 0.0305087264)
        )
        self.df = self.df.with_columns(hr_expr.alias("hr%"))
        return self

    def with_2b_percent(self):
        gap = pl.col("batting_ratings_overall_gap")
        power = pl.col("batting_ratings_overall_power")
        sk = pl.col("batting_ratings_overall_strikeouts")

        # part1_2b: Adjusted gap impact with fudge factor to reduce impact of batting_ratings_overall_gap (0.6666 is the fudge factor)
        # Reduce impact by adjusting the distance to 100
        part1 = (gap - ((gap - 100) * 0.6666)) * 0.0005759923464 + 0.0046460781

        # Function to calculate part2_2b: power-based component with constant subtraction (0.0628)
        part2 = (
            pl.when(power <= 100)
            .then(power * -0.0000508547503 + 0.0669597896 - 0.0628)
            .otherwise(power * -0.00008542726043 + 0.071154717 - 0.0628)
        )

        # Function to calculate part3_2b: strikeouts-based piecewise component with constant subtraction (0.0628)
        part3 = (
            pl.when(sk <= 100)
            .then(sk * -0.0002084865135 + 0.0828934273 - 0.0628)
            .when((sk >= 101) & (sk <= 220))
            .then(sk * -0.000008259599351 + 0.0708287518 - 0.0628)
            .otherwise(0.053 - 0.0628)
        )

        # final 2b% as sum of parts
        double_expr = (part1 + part2 + part3).alias("2b%")
        self.df = self.df.with_columns(double_expr)
        return self

    def with_3b_percent(self):
        gap = pl.col("batting_ratings_overall_gap")
        power = pl.col("batting_ratings_overall_power")
        sk = pl.col("batting_ratings_overall_strikeouts")

        # part1_3b: gap-based component
        part1 = (gap * 0.00004451978242) + 0.00007767274633

        # part2_3b: power-based component with constant subtraction (0.0044)
        part2 = (
            pl.when(power <= 100)
            .then(power * -0.00000206286281 + 0.0046134367 - 0.0044)
            .otherwise(power * -0.000007041275071 + 0.0051236727 - 0.0044)
        )

        # part3_3b: strikeouts-based piecewise component with constant subtraction (0.0044)
        part3 = (
            pl.when(sk <= 100)
            .then(sk * -0.00001098275967 + 0.0055735013 - 0.0044)
            .when((sk >= 101) & (sk <= 220))
            .then(sk * -0.00000526736139 + 0.0048976614 - 0.0044)
            .otherwise(0.0037 - 0.0044)
        )

        # final 3b% as sum of parts
        triple_expr = (part1 + part2 + part3).alias("3b%")
        self.df = self.df.with_columns(triple_expr)
        return self

    def with_1b_percent(self):
        babip = pl.col("batting_ratings_overall_babip")
        gap = pl.col("batting_ratings_overall_gap")
        sk = pl.col("batting_ratings_overall_strikeouts")

        # part1_1b: babip-based component
        part1 = (
            pl.when(babip <= 100)
            .then(babip * 0.0015140038 + 0.1281801944)
            .otherwise(babip * 0.000964994955 + 0.1837822012)
        )

        # part2_1b: gap-based component with constant subtraction (0.28)
        part2 = (gap * -0.0003887320573) + 0.3178756912 - 0.28

        # part3_1b: strikeouts-based piecewise component with constant subtraction (0.28)
        part3 = (
            pl.when(sk <= 100)
            .then(sk * 0.000149985378 + 0.2648525907 - 0.28)
            .when((sk >= 101) & (sk <= 220))
            .then(sk * 0.00005179135613 + 0.2754044069 - 0.28)
            .otherwise(0.286 - 0.28)
        )

        # final 1b% as sum of parts
        single_expr = (part1 + part2 + part3).alias("1b%")
        self.df = self.df.with_columns(single_expr)
        return self

    def lazy(self) -> pl.LazyFrame:
        return self.df
