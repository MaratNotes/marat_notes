class Solution:
    def hasCycle(self, head):
        slow = head # черепаха
        fast = head # заяц
        
        while fast and fast.next: # Пока текущий и следующий узел существуют
            slow = slow.next      # +1 узел
            fast = fast.next.next # +2 узла
            
            if slow == fast:      # Узлы встретились -> цикл
                return True
        
        return False

        