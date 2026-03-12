from src.core.container import build_container
from src.config import OnelineConfig    
import warnings
import argparse
warnings.filterwarnings("ignore")

def main(args):
    service = build_container(oneline_config=OnelineConfig)
    print('[INFO] Process started !')
    service['train_service'].run()
    print('[INFO] Process finished !')

if __name__ == "__main__":
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument('--audio_path', type=str, required=True)
    cli_args = cli_parser.parse_args()
    main(cli_args)