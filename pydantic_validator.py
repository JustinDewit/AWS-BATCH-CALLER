from pydantic import BaseModel


class UrlRulesModel(BaseModel):
    """
    Validates that the input is a list of included and excluded words in string format.
    """

    included_words: list[str]
    excluded_words: list[str]