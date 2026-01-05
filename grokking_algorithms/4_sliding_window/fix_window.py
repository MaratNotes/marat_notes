def max_sum_subarray_fixed_window(arr, k):
    """
    Найти максимальную сумму подмассива фиксированной длины k.
    
    Время: O(n), Память: O(1)
    """
    if k <= 0 or k > len(arr):
        return 0
    
    # Инициализация: сумма первого окна
    window_sum = sum(arr[:k])
    max_sum = window_sum
    
    # Скользим окном от k до конца массива
    for i in range(k, len(arr)):
        window_sum += arr[i] - arr[i - k]  # вычитаем уходящий, прибавляем входящий
        max_sum = max(max_sum, window_sum)
    
    return max_sum


if __name__ == "__main__":
    # Пример из презентации
    arr = [1, 3, 2, 6, -1, 4, 1]
    k = 3
    
    result = max_sum_subarray_fixed_window(arr, k)
    print(f"Массив: {arr}")
    print(f"Размер окна: {k}")
    print(f"Максимальная сумма подмассива длины {k}: {result}")
    # Ожидаемый вывод: 11 (подмассив [3, 2, 6])