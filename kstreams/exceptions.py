class DuplicateStreamException(Exception):
    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        return f"{class_name} {self.name}"

    def __str__(self) -> str:
        msg = (
            f"Duplicate Stream found with name {self.name} "
            "Make sure that the name is unique"
        )

        return msg
