class AcceptedSolution:
    def __init__(self,runtime_stat:str,memory_stat:str):
        self.is_accepted=True
        self.runtime_stat=runtime_stat
        self.memory_stat=memory_stat
class RejectedSolution:
    """Error types are: TimeError,MemoryError,RuntimeError"""
    def __init__(self,error_type:str,error_message:str):
        self.is_accepted=False
        self.error_type=error_type
        self.error_message=error_message