import os
from cerebras.cloud.sdk import Cerebras


class LLMClient:
    def __init__(self, model=None, base_url=None):
        # Model and API key from environment variables
        self.model = os.getenv("CEREBRAS_MODEL", "llama-3.3-70b")
        self.client = Cerebras(
            api_key=os.getenv("CEREBRAS_API_KEY")
        )

    def generate(self, prompt: str) -> str:
        response = self.client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model=self.model,
            max_completion_tokens=1024,
            temperature=0.1,
            top_p=1,
            stream=False
        )
        return response.choices[0].message.content