# from openai import OpenAI
# import os


# client = OpenAI(api_key = "")


# response = client.chat.completions.create(
#     model="gpt-4o-mini",
#     messages=[
#         {"role": "user", "content": "Write a haiku about AI."}
#     ]
# )

# print(response.choices[0].message.content)

from openai import OpenAI
from dotenv import load_dotenv
import os

load_dotenv()

client = OpenAI(api_key=os.getenv("OPEN_AI_KEY"))

response = client.responses.create(
    model="gpt-4o-mini",
    input="Give me a random sentence"
)

print(response.output_text)
