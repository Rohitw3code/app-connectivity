import json
import os
from pathlib import Path

from dotenv import load_dotenv

from llm_client import call_llm, extract_text_from_response, parse_bool


def main() -> None:
    load_dotenv(dotenv_path=Path(__file__).with_name(".env"), override=False)

    vm_mode = parse_bool(os.getenv("VM"), default=False)
    script_path = os.getenv("LLM_SCRIPT_PATH") or str(Path(__file__).with_name("llm_client.bat"))
    api_key = os.getenv("OPENAI_API_KEY", "")

    if not vm_mode and not api_key:
        raise SystemExit(
            "ERROR: OPENAI_API_KEY is required when VM mode is false. "
            "Set OPENAI_API_KEY or set VM=true."
        )

    prompt_data = {
        "messages": [
            {
                "role": "system",
                "content": "You are an AI assistant",
            },
            {
                "role": "user",
                "content": "hii how are you",
            },
        ],
        "temperature": 0.3,
        "max_tokens": 2000,
    }

    with open("prompt.json", "w", encoding="utf-8") as f:
        json.dump(prompt_data, f, indent=2, ensure_ascii=False)

    llm_response = call_llm(
        prompt_payload=prompt_data,
        vm=vm_mode,
        api_key=api_key,
        script_path=script_path,
    )

    print(f"VM mode: {vm_mode}")
    print("LLM Response JSON:", llm_response)
    print("Assistant text:", extract_text_from_response(llm_response))


if __name__ == "__main__":
    main()
