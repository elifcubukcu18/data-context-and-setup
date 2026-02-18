from pathlib import Path
import pandas as pd


class Olist:
    """
    The Olist class provides methods to interact with Olist's e-commerce data.

    Methods:
        get_data():
            Loads and returns a dictionary where keys are dataset names (e.g., 'sellers', 'orders')
            and values are pandas DataFrames loaded from corresponding CSV files.

        ping():
            Prints "pong" to confirm the method is callable.
    """
    def get_data(self):

        csv_path = Path("~/.workintech/olist/data/csv").expanduser()

        file_paths = sorted(csv_path.iterdir())


        names = []
        for file in file_paths:
            name = file.name.removesuffix(".csv")
            name = name.removesuffix("_dataset")
            if name.startswith("olist_"):
                name = name[len("olist_"):]

            names.append(name)

        dataframes = [pd.read_csv(file) for file in file_paths]
        data = dict(zip(names, dataframes))

        return data



    def ping(self):
        """
        You call ping I print pong.
        """
        print("pong")
