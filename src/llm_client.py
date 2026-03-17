import os
from cerebras.cloud.sdk import Cerebras


class LLMClient:
    def __init__(self, model=None, base_url=None):
        # Model and API key from environment variables
        self.model = os.getenv("CEREBRAS_MODEL", "llama-3.1-8b")
        api_key = os.getenv("CEREBRAS_API_KEY")
        if not api_key:
            raise ValueError("CEREBRAS_API_KEY environment variable not set")
        self.client = Cerebras(api_key=api_key)

    def generate(self, prompt: str) -> str:
        """
        Generate a response from the LLM.
        Raises exceptions on API errors rather than silently failing.
        """
        try:
            response = self.client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model=self.model,
                max_completion_tokens=1024,
                temperature=0.1,
                top_p=1,
                stream=False
            )
            return response.choices[0].message.content
        except Exception as e:
            # Re-raise the exception to surface the real error
            # Don't silently cache it or return a default response
            raise