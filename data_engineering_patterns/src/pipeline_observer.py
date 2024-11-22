from abc import ABC, abstractmethod
from typing import List
from datetime import datetime

class PipelineObserver(ABC):
    """Abstract base class for pipeline observers"""
    
    @abstractmethod
    def update(self, event_type: str, message: str):
        pass

class EmailNotifier(PipelineObserver):
    """Concrete observer that sends email notifications"""
    
    def update(self, event_type: str, message: str):
        # In production, implement actual email sending logic
        print(f"Sending email notification - Event: {event_type}, Message: {message}")

class LoggingObserver(PipelineObserver):
    """Concrete observer that logs pipeline events"""
    
    def update(self, event_type: str, message: str):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {event_type}: {message}")

class MetricsCollector(PipelineObserver):
    """Concrete observer that collects pipeline metrics"""
    
    def __init__(self):
        self.metrics = {}

    def update(self, event_type: str, message: str):
        if event_type not in self.metrics:
            self.metrics[event_type] = 0
        self.metrics[event_type] += 1

class DataPipeline:
    """Observable data pipeline that notifies observers of its events"""
    
    def __init__(self):
        self._observers: List[PipelineObserver] = []
        self.data = None

    def attach(self, observer: PipelineObserver):
        """Attach an observer to the pipeline"""
        self._observers.append(observer)

    def detach(self, observer: PipelineObserver):
        """Detach an observer from the pipeline"""
        self._observers.remove(observer)

    def notify(self, event_type: str, message: str):
        """Notify all observers of an event"""
        for observer in self._observers:
            observer.update(event_type, message)

    def process_data(self, data):
        """Example data processing method that notifies observers"""
        self.notify("STARTED", "Data processing started")
        
        try:
            # Simulate data processing
            self.data = data
            self.notify("PROGRESS", "Data transformation in progress")
            
            # More processing steps...
            self.notify("COMPLETED", "Data processing completed successfully")
            
        except Exception as e:
            self.notify("ERROR", f"Error processing data: {str(e)}")

# Usage example
if __name__ == "__main__":
    # Create pipeline and observers
    pipeline = DataPipeline()
    email_notifier = EmailNotifier()
    logger = LoggingObserver()
    metrics_collector = MetricsCollector()

    # Attach observers to pipeline
    pipeline.attach(email_notifier)
    pipeline.attach(logger)
    pipeline.attach(metrics_collector)

    # Process some data
    pipeline.process_data({"example": "data"})

    # Check metrics
    print("\nMetrics collected:")
    print(metrics_collector.metrics)
