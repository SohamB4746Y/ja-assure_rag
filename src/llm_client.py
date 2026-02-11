import requests


class LLMClient:
    def __init__(self, model="llama3:8b", base_url="http://localhost:11434"):
        self.model = model
        self.url = f"{base_url}/api/generate"

    def generate(self, prompt: str) -> str:
        response = requests.post(
            self.url,
            json={
                "model": self.model,
                "prompt": prompt,
                "stream": False
            }
        )

        if response.status_code != 200:
            raise Exception(f"LLM request failed: {response.text}")

        return response.json()["response"]