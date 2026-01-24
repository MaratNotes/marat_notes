def next_greater_element(arr):
    n = len(arr)
    res = [-1] * n          # Шаг 1: инициализация
    stack = []              # будем хранить индексы

    for i in range(n):      # Шаг 2–3: проход по массиву
        # Пока стек не пуст и текущий элемент больше вершины
        while stack and arr[i] > arr[stack[-1]]:
            j = stack.pop()       # достаём индекс
            res[j] = arr[i]       # записываем ответ
        stack.append(i)           # добавляем текущий индекс

    return res              # Шаг 4: завершение