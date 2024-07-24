import apache_beam as beam
from apache_beam import DoFn, PCollection, ParDo
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window
from typing import List, Tuple, Dict
import logging
import json
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = os.environ.get('PROJECT_ID')
BUCKET = os.environ.get('BUCKET')

if not PROJECT_ID or not BUCKET:
    raise ValueError("PROJECT_ID and BUCKET must be set as environment variables")

class BookReview(beam.typehints.NamedTuple):
    book_id: str
    title: str
    author: str
    review_text: str
    rating: float
    timestamp: int

class CleanAndValidateReview(DoFn):
    def process(self, element: Dict) -> List[BookReview]:
        try:
            # Convert dict to BookReview object
            review = BookReview(
                book_id=element['book_id'],
                title=element['title'].strip(),
                author=element['author'].strip(),
                review_text=element['review_text'].strip(),
                rating=float(element['rating']),
                timestamp=int(element['timestamp'])
            )
            
            # Validate rating
            if not (0 <= review.rating <= 5):
                logger.warning(f"Invalid rating for book {review.book_id}: {review.rating}")
                return []
            
            return [review]
        except KeyError as e:
            logger.error(f"Missing field in review: {e}")
        except ValueError as e:
            logger.error(f"Invalid value in review: {e}")
        return []  # Return empty list if there's an error

class AnalyzeReviews(beam.PTransform):
    def expand(self, pcoll: PCollection) -> PCollection:
        return (
            pcoll
            | "Extract Rating" >> beam.Map(lambda x: (x.book_id, x.rating))
            | "Group By Book" >> beam.GroupByKey()
            | "Calculate Average" >> beam.Map(lambda x: (x[0], sum(x[1]) / len(x[1])))
        )

def run_pipeline():
    # Pipeline options
    options = PipelineOptions(
        runner='DataflowRunner',
        project=PROJECT_ID,
        job_name='book-review-analysis',
        temp_location=f'gs://{BUCKET}/temp',
        region='us-central1',
        max_num_workers=4
    )

    book_categories = {
        'book1': 'Fiction',
        'book2': 'Non-fiction',
    }

    input_path = f"gs://{BUCKET}/input/book-reviews.json"
    output_path = f"gs://{BUCKET}/output/book-review-analysis"

    with beam.Pipeline(options=options) as p:
        # Read input
        reviews = (
            p | "Read JSON" >> beam.io.ReadFromText(input_path)
            | "Parse JSON" >> beam.Map(json.loads)
            | "Clean and Validate" >> ParDo(CleanAndValidateReview())
        )

        windowed_reviews = (
            reviews
            | "Add Timestamps" >> beam.Map(lambda x: beam.window.TimestampedValue(x, x.timestamp))
            | "Window" >> beam.WindowInto(window.FixedWindows(60 * 60))  # 1-hour windows
        )

        average_ratings = windowed_reviews | "Analyze Reviews" >> AnalyzeReviews()

        def add_category(element: Tuple[str, float], categories: Dict[str, str]) -> Tuple[str, float, str]:
            book_id, avg_rating = element
            category = categories.get(book_id, 'Unknown')
            return (book_id, avg_rating, category)

        categorized_ratings = (
            average_ratings
            | "Add Category" >> beam.Map(
                add_category, 
                categories=beam.pvalue.AsDict(p | beam.Create(book_categories.items()))
            )
        )

        _ = (
            categorized_ratings
            | "Format Output" >> beam.Map(lambda x: f"{x[0]},{x[1]:.2f},{x[2]}")
            | "Write CSV" >> beam.io.WriteToText(
                output_path,
                file_name_suffix='.csv',
                header='book_id,average_rating,category'
            )
        )

if __name__ == "__main__":
    run_pipeline()
