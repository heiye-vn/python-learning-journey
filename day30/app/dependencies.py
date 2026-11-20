from day30.app.repository import TodoRepository

todo_repository = TodoRepository()


def get_todo_repository() -> TodoRepository:
    return todo_repository
