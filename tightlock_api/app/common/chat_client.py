"""
Copyright 2023 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

import common.airflow_client as airflow_client_lib
import vertexai
from vertexai.language_models import ChatModel, ChatMessage

_MODEL = 'chat-bison@001'


class ChatClient:
  """A client to converse with the VertexAI chat model."""

  def __init__(self) -> None:
    """Initializes the LogChatClient.
    
    Args:
      logs: A dictionary of the last log entries for all available dags.
    """
    vertexai.init(project='cse-hack-23-tightlock-nyc')
    self._client = ChatModel.from_pretrained(_MODEL)
    self._logs = airflow_client_lib.AirflowClient().get_latest_logs()
    self._context = (
      'My name is Tightlock. I\'m assisting users in debugging'
      f'log messages. Here are the log messages:\n\n{self._logs}'
    )

  def get_chat_response(
      self,
      message: str,
      chat_history: list[dict[str, str]]|None
    ) -> str:
    """Gets a response from the the chat model.
    
    Args:
      message: The chat message.
      message_history: The chat history to consider.
    
    Returns:
      The response from the LLM.
    """
    message_history = self._build_message_history_from_dict(
      chat_history)
    chat = self._client.start_chat(
      context=self._context,
      message_history=message_history,
      temperature=0.3,
      top_p=0.95,
      top_k=40
    )
    return chat.send_message(message)
  
  def _build_message_history_from_dict(
      self,
      chat_history: list[dict[str, str]]
  ) -> list[ChatMessage]:
    """Build a the message history object.
    
    Args:
      chat_history: A dict of messages in the following form:
        [{
          'author': 'user',
          'content': 'user message'
        }]
      
    """
    message_history = []
    for _ in chat_history:
      message_history.append(
        ChatMessage(
          content=chat_history['content'],
          author=chat_history['author'])
      )
    return message_history

      
