#!/usr/bin/env python3
"""
Production Log Generator for AWS S3

Generates realistic nginx-style log files and uploads them to S3 with proper
partitioning by date. Supports command-line arguments and robust error handling.

Usage:
    python log_generator.py --num-files 100 --lines-per-file 1000 --bucket my-bucket --app nginx
    python log_generator.py -n 50 -l 500 -b my-log-bucket -a apache --start-date 2025-01-01 --end-date 2025-01-31
"""

import os
import sys
import random
import gzip
import boto3
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple, Optional
from faker import Faker
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

# --- Constants ---
DEFAULT_NUM_FILES = 100
DEFAULT_LINES_PER_FILE = 10
DEFAULT_APP_NAME = 'nginx'
DEFAULT_LOCAL_LOG_DIR = 'logs'
DEFAULT_START_DATE = '2025-01-01'
DEFAULT_END_DATE = '2025-01-31'

STATUS_CODES = [200, 201, 301, 400, 403, 404, 500]
URL_PATHS = ["/home", "/login", "/products", "/api/v1/users", "/static/css/main.css", "/Integrated/challenge.gif"]
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15'
]


class LogGeneratorError(Exception):
    """Custom exception for log generator errors."""
    pass


class LogGenerator:
    """Production-ready log file generator with S3 upload capabilities."""
    
    def __init__(self, num_files: int, lines_per_file: int, s3_bucket: str, 
                 app_name: str, start_date: datetime, end_date: datetime, 
                 local_dir: str = DEFAULT_LOCAL_LOG_DIR):
        self.num_files = num_files
        self.lines_per_file = lines_per_file
        self.s3_bucket = s3_bucket
        self.app_name = app_name
        self.start_date = start_date
        self.end_date = end_date
        self.local_dir = Path(local_dir)
        self.fake = Faker()
        
        # Setup logging
        self._setup_logging()
        
        # Validate configuration
        self._validate_config()
        
        # Setup AWS S3 client
        self._setup_s3_client()
        
        # Create local directory
        self.local_dir.mkdir(exist_ok=True)
        
    def _setup_logging(self) -> None:
        """Configure logging with both file and console output."""
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        # Create logger
        self.logger = logging.getLogger('log_generator')
        self.logger.setLevel(logging.INFO)
        
        # Prevent duplicate handlers
        if self.logger.handlers:
            return
            
        # File handler
        file_handler = logging.FileHandler('log_generator.log')
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter(log_format)
        file_handler.setFormatter(file_formatter)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(levelname)s: %(message)s')
        console_handler.setFormatter(console_formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
    def _validate_config(self) -> None:
        """Validate configuration parameters."""
        if self.num_files <= 0:
            raise LogGeneratorError(f"num_files must be positive, got {self.num_files}")
            
        if self.lines_per_file <= 0:
            raise LogGeneratorError(f"lines_per_file must be positive, got {self.lines_per_file}")
            
        if not self.s3_bucket:
            raise LogGeneratorError("S3 bucket name cannot be empty")
            
        if not self.app_name:
            raise LogGeneratorError("App name cannot be empty")
            
        if self.start_date >= self.end_date:
            raise LogGeneratorError(f"start_date ({self.start_date}) must be before end_date ({self.end_date})")
            
        self.logger.info(f"Configuration validated successfully")
        self.logger.info(f"Will generate {self.num_files} files with {self.lines_per_file} lines each")
        self.logger.info(f"Date range: {self.start_date.date()} to {self.end_date.date()}")
        
    def _setup_s3_client(self) -> None:
        """Setup and validate S3 client."""
        try:
            self.s3_client = boto3.client('s3')
            # Test credentials by listing buckets
            self.s3_client.list_buckets()
            self.logger.info("AWS credentials validated successfully")
            
            # Check if bucket exists and is accessible
            try:
                self.s3_client.head_bucket(Bucket=self.s3_bucket)
                self.logger.info(f"S3 bucket '{self.s3_bucket}' is accessible")
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == '404':
                    raise LogGeneratorError(f"S3 bucket '{self.s3_bucket}' does not exist")
                elif error_code == '403':
                    raise LogGeneratorError(f"Access denied to S3 bucket '{self.s3_bucket}'")
                else:
                    raise LogGeneratorError(f"Error accessing S3 bucket: {e}")
                    
        except NoCredentialsError:
            raise LogGeneratorError("AWS credentials not found. Please configure AWS credentials.")
        except Exception as e:
            raise LogGeneratorError(f"Failed to setup S3 client: {e}")
            
    def _random_date_in_range(self) -> datetime:
        """Return a random datetime between start_date and end_date."""
        delta = self.end_date - self.start_date
        random_days = random.randint(0, delta.days)
        return self.start_date + timedelta(days=random_days)
        
    def _generate_log_line(self, base_date: datetime) -> str:
        """Generate a single realistic log line for the given date."""
        try:
            ip = self.fake.ipv4()
            
            # Generate random time within the base_date day
            day_start = datetime(base_date.year, base_date.month, base_date.day, 0, 0, 0)
            day_end = datetime(base_date.year, base_date.month, base_date.day, 23, 59, 59)
            random_time = self.fake.date_time_between_dates(datetime_start=day_start, datetime_end=day_end)
            
            date_str = random_time.strftime("%d/%b/%Y:%H:%M:%S +0000")
            method = random.choice(["GET", "POST", "PUT", "DELETE"])
            url = random.choice(URL_PATHS)
            status = random.choice(STATUS_CODES)
            size = random.randint(500, 5000)
            user_agent = random.choice(USER_AGENTS)
            referer = random.choice(["-", "https://www.google.com/", "https://www.example.com/"])
            
            return f'{ip} - - [{date_str}] "{method} {url} HTTP/1.1" {status} {size} "{referer}" "{user_agent}"\n'
            
        except Exception as e:
            self.logger.error(f"Failed to generate log line: {e}")
            return ''
            
    def _generate_log_file(self, file_index: int) -> Tuple[Path, datetime]:
        """Generate a single gzipped log file."""
        file_date = self._random_date_in_range()
        timestamp = file_date.strftime('%Y%m%dT%H%M%S')
        filename = f'part-{timestamp}_{file_index:06d}.log.gz'
        local_path = self.local_dir / filename
        
        try:
            with gzip.open(local_path, 'wt', encoding='utf-8') as gz_file:
                for line_num in range(self.lines_per_file):
                    log_line = self._generate_log_line(file_date)
                    if log_line:  # Only write non-empty lines
                        gz_file.write(log_line)
                        
            self.logger.debug(f"Generated log file: {local_path}")
            return local_path, file_date
            
        except Exception as e:
            self.logger.error(f"Error generating log file {file_index}: {e}")
            raise LogGeneratorError(f"Failed to generate log file: {e}")
            
    def _upload_to_s3(self, local_path: Path, file_date: datetime) -> None:
        """Upload a file to S3 with proper partitioning."""
        try:
            # Create S3 key with date partitioning
            s3_key = f"logs/{self.app_name}/{file_date.year}/{file_date.month:02d}/{file_date.day:02d}/{local_path.name}"
            
            # Upload file
            self.s3_client.upload_file(str(local_path), self.s3_bucket, s3_key)
            self.logger.debug(f"Uploaded to s3://{self.s3_bucket}/{s3_key}")
            
            # Clean up local file after successful upload
            local_path.unlink()
            
        except (BotoCoreError, ClientError) as e:
            self.logger.error(f"AWS upload failed for {local_path.name}: {e}")
            raise LogGeneratorError(f"S3 upload failed: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected upload error for {local_path.name}: {e}")
            raise LogGeneratorError(f"Upload error: {e}")
            
    def generate_and_upload(self) -> None:
        """Main method to generate all log files and upload to S3."""
        self.logger.info("Starting log generation process")
        
        successful_uploads = 0
        failed_uploads = 0
        
        try:
            for i in range(self.num_files):
                try:
                    # Generate log file
                    local_path, file_date = self._generate_log_file(i)
                    
                    # Upload to S3
                    self._upload_to_s3(local_path, file_date)
                    
                    successful_uploads += 1
                    
                    # Progress logging
                    if (i + 1) % 10 == 0 or i == self.num_files - 1:
                        self.logger.info(f"Progress: {i + 1}/{self.num_files} files processed")
                        
                except LogGeneratorError as e:
                    failed_uploads += 1
                    self.logger.error(f"Failed to process file #{i}: {e}")
                    continue
                except Exception as e:
                    failed_uploads += 1
                    self.logger.error(f"Unexpected error processing file #{i}: {e}")
                    continue
                    
        except KeyboardInterrupt:
            self.logger.warning("Process interrupted by user")
            raise
        except Exception as e:
            self.logger.error(f"Critical error during generation: {e}")
            raise LogGeneratorError(f"Generation failed: {e}")
        finally:
            self.logger.info(f"Generation completed: {successful_uploads} successful, {failed_uploads} failed")


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Generate realistic log files and upload to S3',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --num-files 100 --lines-per-file 1000 --bucket my-logs --app nginx
  %(prog)s -n 50 -l 500 -b my-bucket -a apache --start-date 2025-01-01
  %(prog)s --bucket prod-logs --app web-server --local-dir ./temp-logs
        """
    )
    
    parser.add_argument(
        '-n', '--num-files',
        type=int,
        default=DEFAULT_NUM_FILES,
        help=f'Number of log files to generate (default: {DEFAULT_NUM_FILES})'
    )
    
    parser.add_argument(
        '-l', '--lines-per-file',
        type=int,
        default=DEFAULT_LINES_PER_FILE,
        help=f'Number of log lines per file (default: {DEFAULT_LINES_PER_FILE})'
    )
    
    parser.add_argument(
        '-b', '--bucket',
        type=str,
        required=True,
        help='S3 bucket name for log storage (required)'
    )
    
    parser.add_argument(
        '-a', '--app',
        type=str,
        default=DEFAULT_APP_NAME,
        help=f'Application name for log categorization (default: {DEFAULT_APP_NAME})'
    )
    
    parser.add_argument(
        '--start-date',
        type=str,
        default=DEFAULT_START_DATE,
        help=f'Start date for log generation (YYYY-MM-DD, default: {DEFAULT_START_DATE})'
    )
    
    parser.add_argument(
        '--end-date',
        type=str,
        default=DEFAULT_END_DATE,
        help=f'End date for log generation (YYYY-MM-DD, default: {DEFAULT_END_DATE})'
    )
    
    parser.add_argument(
        '--local-dir',
        type=str,
        default=DEFAULT_LOCAL_LOG_DIR,
        help=f'Local directory for temporary file storage (default: {DEFAULT_LOCAL_LOG_DIR})'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    return parser.parse_args()


def parse_date(date_string: str) -> datetime:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_string, '%Y-%m-%d')
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_string}. Use YYYY-MM-DD")


def main() -> int:
    """Main entry point."""
    try:
        # Parse command line arguments
        args = parse_arguments()
        
        # Parse dates
        start_date = parse_date(args.start_date)
        end_date = parse_date(args.end_date)
        
        # Adjust logging level if verbose
        if args.verbose:
            logging.getLogger('log_generator').setLevel(logging.DEBUG)
        
        # Create and run log generator
        generator = LogGenerator(
            num_files=args.num_files,
            lines_per_file=args.lines_per_file,
            s3_bucket=args.bucket,
            app_name=args.app,
            start_date=start_date,
            end_date=end_date,
            local_dir=args.local_dir
        )
        
        generator.generate_and_upload()
        
        print(f"\n✅ Successfully completed log generation!")
        print(f"Generated {args.num_files} files with {args.lines_per_file} lines each")
        print(f"Uploaded to s3://{args.bucket}/logs/{args.app}/")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n❌ Process interrupted by user")
        return 1
    except LogGeneratorError as e:
        print(f"❌ Error: {e}")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())