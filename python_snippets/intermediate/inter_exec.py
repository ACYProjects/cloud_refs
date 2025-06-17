# Description
# Given a list of intervals represented as tuples (start, end), merge all overlapping intervals and return a list of non-overlapping intervals.
# Example:

# Input: [(1, 3), (2, 6), (8, 10), (15, 18)]
# Output: [(1, 6), (8, 10), (15, 18)]

# Constraints:

# Intervals are not necessarily sorted
# Each interval has start <= end
# Return intervals in sorted order

def merge_intervals(intervals):
    if not intervals:
        return []
    
    # Sort intervals by start time
    intervals.sort(key=lambda x: x[0])
    
    merged = [intervals[0]]
    
    for current in intervals[1:]:
        last_merged = merged[-1]
        
        # If current interval overlaps with the last merged interval
        if current[0] <= last_merged[1]:
            # Merge by updating the end time
            merged[-1] = (last_merged[0], max(last_merged[1], current[1]))
        else:
            # No overlap, add current interval
            merged.append(current)
    
    return merged


# Description
# Given the root of a binary tree, return a list of lists where each inner list contains the values of nodes at that level, from left to right.
# Example:
#     3
#    / \
#   9   20
#      /  \
#     15   7

# Output: [[3], [9, 20], [15, 7]]


from collections import deque

class TreeNode:
    def __init__(self, val=0, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right

def level_order_traversal(root):
    if not root:
        return []
    
    result = []
    queue = deque([root])
    
    while queue:
        level_size = len(queue)
        current_level = []
        
        # Process all nodes at current level
        for _ in range(level_size):
            node = queue.popleft()
            current_level.append(node.val)
            
            # Add children to queue for next level
            if node.left:
                queue.append(node.left)
            if node.right:
                queue.append(node.right)
        
        result.append(current_level)
    
    return result


# Description
# Given an integer array nums, return an array answer such that answer[i] is equal to the product of all elements in nums except nums[i].
# Constraints:

# You must solve it without using division
# The algorithm should run in O(n) time
# Use only O(1) extra space (not counting the output array)

# Example:

# Input: [1, 2, 3, 4]
# Output: [24, 12, 8, 6]


def product_except_self(nums):
    n = len(nums)
    result = [1] * n
    
    # First pass: calculate left products
    # result[i] will contain product of all elements to the left of i
    for i in range(1, n):
        result[i] = result[i-1] * nums[i-1]
    
    # Second pass: multiply with right products
    # right_product keeps track of product of elements to the right
    right_product = 1
    for i in range(n-1, -1, -1):
        result[i] *= right_product
        right_product *= nums[i]
    
    return result

# Description
# Given a string containing only parentheses characters '(', ')', '{', '}', '[', ']', determine if the input string has valid parentheses. The string is valid if:

# Open brackets are closed by the same type of brackets
# Open brackets are closed in the correct order
# Every close bracket has a corresponding open bracket

# Example:

# Input: "()[]{}"  → Output: True
# Input: "([)]"    → Output: False
# Input: "{[]}"    → Output: True


def is_valid_parentheses(s):
    stack = []
    mapping = {')': '(', '}': '{', ']': '['}
    
    for char in s:
        if char in mapping:  # Closing bracket
            if not stack or stack.pop() != mapping[char]:
                return False
        else:  # Opening bracket
            stack.append(char)
    
    return len(stack) == 0

# Description
# Given a string s, find the length of the longest substring without repeating characters.
# Example:

# Input: "abcabcbb"  → Output: 3 (substring "abc")
# Input: "bbbbb"     → Output: 1 (substring "b")
# Input: "pwwkew"    → Output: 3 (substring "wke")

# Constraints:

# 0 <= s.length <= 50000
# s consists of English letters, digits, symbols and spaces


def length_of_longest_substring(s):
    if not s:
        return 0
    
    char_index = {}  # Maps character to its most recent index
    left = 0  # Left pointer of sliding window
    max_length = 0
    
    for right in range(len(s)):
        char = s[right]
        
        # If character is repeated and within current window
        if char in char_index and char_index[char] >= left:
            left = char_index[char] + 1
        
        char_index[char] = right
        max_length = max(max_length, right - left + 1)
    
    return max_length

# Given an array of strings strs, group the anagrams together. You can return the answer in any order.
# An anagram is a word formed by rearranging the letters of another word, using all original letters exactly once.
# Example:

# Input: ["eat","tea","tan","ate","nat","bat"]
# Output: [["bat"],["nat","tan"],["ate","eat","tea"]]

# Constraints:

# 1 <= strs.length <= 10^4
# 0 <= strs[i].length <= 100
# strs[i] consists of lowercase English letters only

from collections import defaultdict

def group_anagrams(strs):
    anagram_groups = defaultdict(list)
    
    for s in strs:
        # Sort characters to create a key for anagrams
        # All anagrams will have the same sorted key
        key = ''.join(sorted(s))
        anagram_groups[key].append(s)
    
    return list(anagram_groups.values())

# Alternative solution using character frequency as key
def group_anagrams_v2(strs):
    anagram_groups = defaultdict(list)
    
    for s in strs:
        # Create frequency count as tuple (immutable, can be dict key)
        char_count = [0] * 26
        for char in s:
            char_count[ord(char) - ord('a')] += 1
        
        key = tuple(char_count)
        anagram_groups[key].append(s)
    
    return list(anagram_groups.values())


# Exercise 7: Rotate Matrix 90 Degrees
# Description
# Given an n × n 2D matrix representing an image, rotate the matrix by 90 degrees clockwise in-place. You have to rotate the image in-place, meaning you have to modify the input 2D matrix directly without allocating another 2D matrix.
# Example:
# Input:           Output:
# [1,2,3]         [7,4,1]
# [4,5,6]   -->   [8,5,2]
# [7,8,9]         [9,6,3]
# Constraints:

# n == matrix.length == matrix[i].length
# 1 <= n <= 20
# -1000 <= matrix[i][j] <= 1000


def rotate_matrix(matrix):
    n = len(matrix)
    
    # Step 1: Transpose the matrix (swap matrix[i][j] with matrix[j][i])
    for i in range(n):
        for j in range(i, n):
            matrix[i][j], matrix[j][i] = matrix[j][i], matrix[i][j]
    
    # Step 2: Reverse each row
    for i in range(n):
        matrix[i].reverse()
    
    return matrix

# Alternative one-pass solution
def rotate_matrix_v2(matrix):
    n = len(matrix)
    
    # Process each layer of the matrix
    for layer in range(n // 2):
        first = layer
        last = n - 1 - layer
        
        for i in range(first, last):
            offset = i - first
            
            # Save top element
            top = matrix[first][i]
            
            # Move left to top
            matrix[first][i] = matrix[last - offset][first]
            
            # Move bottom to left
            matrix[last - offset][first] = matrix[last][last - offset]
            
            # Move right to bottom
            matrix[last][last - offset] = matrix[i][last]
            
            # Move top to right
            matrix[i][last] = top

# Description
# A peak element is an element that is strictly greater than its neighbors. Given a 0-indexed integer array nums, find a peak element and return its index. If the array contains multiple peaks, return the index to any of the peaks.
# You may imagine that nums[-1] = nums[n] = -∞. In other words, an element is always considered to be strictly greater than a neighbor that is outside the array.
# You must write an algorithm that runs in O(log n) time.
# Example:

# Input: nums = [1,2,3,1] → Output: 2 (nums[2] = 3 is a peak)
# Input: nums = [1,2,1,3,5,6,4] → Output: 5 (nums[5] = 6 is a peak)


def find_peak_element(nums):
    left, right = 0, len(nums) - 1
    
    while left < right:
        mid = (left + right) // 2
        
        # If mid element is smaller than its right neighbor,
        # there must be a peak on the right side
        if nums[mid] < nums[mid + 1]:
            left = mid + 1
        else:
            # If mid >= nums[mid + 1], peak is on left side or mid itself
            right = mid
    
    return left

# Alternative approach with explicit boundary checks
def find_peak_element_v2(nums):
    n = len(nums)
    
    # Handle edge cases
    if n == 1:
        return 0
    if nums[0] > nums[1]:
        return 0
    if nums[n-1] > nums[n-2]:
        return n - 1
    
    # Binary search in the middle elements
    left, right = 1, n - 2
    
    while left <= right:
        mid = (left + right) // 2
        
        # Check if mid is a peak
        if nums[mid] > nums[mid-1] and nums[mid] > nums[mid+1]:
            return mid
        elif nums[mid] < nums[mid-1]:
            right = mid - 1
        else:
            left = mid + 1
    
    return -1  # Should never reach here given problem constraints

# Description
# You are given an integer array coins representing coins of different denominations and an integer amount representing a total amount of money. Return the fewest number of coins that you need to make up that amount. If that amount of money cannot be made up by any combination of the coins, return -1.
# You may assume that you have an infinite number of each kind of coin.
# Example:

# Input: coins = [1,3,4], amount = 6 → Output: 2 (6 = 3 + 3)
# Input: coins = [2], amount = 3 → Output: -1
# Input: coins = [1], amount = 0 → Output: 0

def coin_change(coins, amount):
    # dp[i] represents minimum coins needed to make amount i
    dp = [float('inf')] * (amount + 1)
    dp[0] = 0  # 0 coins needed to make amount 0
    
    for i in range(1, amount + 1):
        for coin in coins:
            if coin <= i:
                dp[i] = min(dp[i], dp[i - coin] + 1)
    
    return dp[amount] if dp[amount] != float('inf') else -1

# Space-optimized BFS approach
from collections import deque

def coin_change_bfs(coins, amount):
    if amount == 0:
        return 0
    
    visited = set()
    queue = deque([(0, 0)])  # (current_amount, num_coins)
    
    while queue:
        curr_amount, num_coins = queue.popleft()
        
        for coin in coins:
            new_amount = curr_amount + coin
            
            if new_amount == amount:
                return num_coins + 1
            
            if new_amount < amount and new_amount not in visited:
                visited.add(new_amount)
                queue.append((new_amount, num_coins + 1))
    
    return -1

# Given the head of a linked list, determine if the linked list has a cycle in it. A cycle exists if some node in the list can be reached again by continuously following the next pointer.
# Return True if there is a cycle in the linked list, otherwise return False.
# Follow-up: Can you find the start of the cycle if it exists?
# Example:

# Input: head = [3,2,0,-4] with cycle at position 1 → Output: True
# Input: head = [1,2] with no cycle → Output: False

class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next

def has_cycle(head):
    """Floyd's Cycle Detection Algorithm (Tortoise and Hare)"""
    if not head or not head.next:
        return False
    
    slow = head
    fast = head
    
    while fast and fast.next:
        slow = slow.next
        fast = fast.next.next
        
        if slow == fast:
            return True
    
    return False

def detect_cycle_start(head):
    """Find the node where the cycle begins"""
    if not head or not head.next:
        return None
    
    # Phase 1: Detect if cycle exists
    slow = fast = head
    
    while fast and fast.next:
        slow = slow.next
        fast = fast.next.next
        
        if slow == fast:
            break
    else:
        return None  # No cycle found


Description
# Given the root of a binary tree, determine if it is a valid binary search tree (BST).
# A valid BST is defined as follows:

# The left subtree of a node contains only nodes with keys less than the node's key
# The right subtree of a node contains only nodes with keys greater than the node's key
# Both left and right subtrees must also be binary search trees

# Example:
#     2          5
#    / \        / \
#   1   3      1   4
#             / \
#            3   6

# First tree: Valid BST
# Second tree: Invalid BST (3 < 5 but 3 is in right subtree)

class TreeNode:
    def __init__(self, val=0, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right

def is_valid_bst(root):
    """Validate BST using bounds checking"""
    def validate(node, min_val, max_val):
        if not node:
            return True
        
        if node.val <= min_val or node.val >= max_val:
            return False
        
        return (validate(node.left, min_val, node.val) and 
                validate(node.right, node.val, max_val))
    
    return validate(root, float('-inf'), float('inf'))

def is_valid_bst_inorder(root):
    """Alternative: Use inorder traversal (should be sorted for valid BST)"""
    def inorder(node):
        if not node:
            return []
        return inorder(node.left) + [node.val] + inorder(node.right)
    
    values = inorder(root)
    return all(values[i] < values[i+1] for i in range(len(values)-1))

def is_valid_bst_iterative(root):
    """Iterative inorder with early termination"""
    if not root:
        return True
    
    stack = []
    prev_val = float('-inf')
    current = root
    
    while stack or current:
        # Go to leftmost node
        while current:
            stack.append(current)
            current = current.left
        
        # Process current node
        current = stack.pop()
        
        if current.val <= prev_val:
            return False
        
        prev_val = current.val
        current = current.right
    
    return True

# Description
# A transformation sequence from word beginWord to word endWord using a dictionary wordList is a sequence of words such that:

# The first word is beginWord
# The last word is endWord
# Only one letter is different between each adjacent pair of words
# Every intermediate word must be in wordList

# Return the length of the shortest transformation sequence. If no such sequence exists, return 0.


from collections import deque, defaultdict

def ladder_length(begin_word, end_word, word_list):
    if end_word not in word_list:
        return 0
    
    # Create adjacency list of words that differ by one character
    word_set = set(word_list)
    if begin_word in word_set:
        word_set.remove(begin_word)
    
    queue = deque([(begin_word, 1)])  # (word, length)
    visited = {begin_word}
    
    while queue:
        current_word, length = queue.popleft()
        
        # Try changing each character
        for i in range(len(current_word)):
            for c in 'abcdefghijklmnopqrstuvwxyz':
                if c == current_word[i]:
                    continue
                
                new_word = current_word[:i] + c + current_word[i+1:]
                
                if new_word == end_word:
                    return length + 1
                
                if new_word in word_set and new_word not in visited:
                    visited.add(new_word)
                    queue.append((new_word, length + 1))
    
    return 0

def ladder_length_bidirectional(begin_word, end_word, word_list):
    """Optimized bidirectional BFS"""
    if end_word not in word_list:
        return 0
    
    word_set = set(word_list)
    
    # Two sets for bidirectional search
    begin_set = {begin_word}
    end_set = {end_word}
    visited = set()
    length = 1
    
    while begin_set and end_set:
        # Always search from the smaller set
        if len(begin_set) > len(end_set):
            begin_set, end_set = end_set, begin_set
        
        next_set = set()
        
        for word in begin_set:
            for i in range(len(word)):
                for c in 'abcdefghijklmnopqrstuvwxyz':
                    if c == word[i]:
                        continue
                    
                    new_word = word[:i] + c + word[i+1:]
                    
                    if new_word in end_set:
                        return length + 1
                    
                    if new_word in word_set and new_word not in visited:
                        visited.add(new_word)
                        next_set.add(new_word)
        
        begin_set = next_set
        length += 1
    
    return 0
