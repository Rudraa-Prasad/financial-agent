#import dotenv
dotenv.load_dotenv()


from chatbot_api.src.chains.finance_review_chain import (
    reviews_vector_chain
)

query = """"Are there any negative reviews.
        Mention details from specific reviews."""

response = reviews_vector_chain.invoke(query)

d = response.get("result")
print(d)