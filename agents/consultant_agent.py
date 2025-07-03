from crewai import Agent
from llm_setup import gpt4
from crewai.tools import BaseTool
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.cluster import KMeans
from prophet import Prophet
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import IsolationForest
from typing import Optional, List, Type
from pydantic import BaseModel
import traceback

class PandasAnalysisArgs(BaseModel):
    data: dict
    analysis_type: str = "summary"
    target: Optional[str] = None
    features: Optional[List[str]] = None
    n_clusters: int = 3

class PandasAnalysisTool(BaseTool):
    args_schema: Type[PandasAnalysisArgs] = PandasAnalysisArgs
    model_config = {"arbitrary_types_allowed": True}
    def _run(self, data: dict, analysis_type: str = "summary", target: Optional[str] = None, features: Optional[List[str]] = None, n_clusters: int = 3, user_input: Optional[str] = None):
        print("DEBUG: PandasAnalysisTool _run called")
        print(f"DEBUG: data={{'keys': list(data.keys())}}, analysis_type={analysis_type}, target={target}, features={features}, n_clusters={n_clusters}, user_input={user_input}")
        # Automatische analysekeuze op basis van user_input
        if (not analysis_type or analysis_type == "summary") and user_input:
            lowered = user_input.lower()
            if any(word in lowered for word in ["trend", "ontwikkeling"]):
                analysis_type = "trend"
            elif any(word in lowered for word in ["correlatie", "verband"]):
                analysis_type = "correlation"
            elif any(word in lowered for word in ["regressie", "voorspel", "predict"]):
                analysis_type = "regression"
            elif any(word in lowered for word in ["cluster", "segment"]):
                analysis_type = "clustering"
            elif any(word in lowered for word in ["classificatie", "categorie", "label"]):
                analysis_type = "classification"
            elif any(word in lowered for word in ["anomalie", "outlier", "afwijking"]):
                analysis_type = "anomaly"
            else:
                analysis_type = "summary"
        try:
            if all(not isinstance(v, (list, dict, tuple, set)) for v in data.values()):
                # Alle waarden zijn scalars: maak een DataFrame met één rij
                df = pd.DataFrame([data])
            else:
                df = pd.DataFrame(data)
            # Kolom-check: filter features/target op bestaande kolommen
            if features:
                features = [f for f in features if f in df.columns]
                if not features:
                    return "Geen van de gevraagde features bestaat in de data."
            if target and target not in df.columns:
                return f"Target '{target}' bestaat niet in de data."
            if analysis_type == "summary":
                return df.describe().to_dict()
            elif analysis_type == "trend":
                return (df.iloc[-1] - df.iloc[0]).to_dict()
            elif analysis_type == "correlation":
                return df.corr().to_dict()
            elif analysis_type == "regression":
                if not target or not features:
                    return "Voor regressie zijn target en features vereist."
                X = df[features]
                y = df[target]
                model = LinearRegression().fit(X, y)
                return {"coefs": model.coef_.tolist(), "intercept": model.intercept_}
            elif analysis_type == "clustering":
                if not features:
                    return "Voor clustering zijn features vereist."
                X = df[features]
                kmeans = KMeans(n_clusters=n_clusters).fit(X)
                labels = kmeans.labels_.tolist() if kmeans.labels_ is not None else []
                centers = kmeans.cluster_centers_.tolist() if kmeans.cluster_centers_ is not None else []
                return {"labels": labels, "centers": centers}
            elif analysis_type == "classification":
                if not target or not features:
                    return "Voor classificatie zijn target en features vereist."
                X = df[features]
                y = df[target]
                model = RandomForestClassifier().fit(X, y)
                return {"feature_importances": model.feature_importances_.tolist()}
            elif analysis_type == "anomaly":
                if not features:
                    return "Voor anomaly detection zijn features vereist."
                X = df[features]
                model = IsolationForest().fit(X)
                return {"anomaly_scores": model.decision_function(X).tolist()}
            else:
                return f"Onbekend analysis_type: {analysis_type}"
        except Exception as e:
            print("DEBUG: PandasAnalysisTool except-blok bereikt!")
            print("ERROR in PandasAnalysisTool:", e)
            traceback.print_exc()
            return f"TOOL ERROR: {e}"

pandas_tool = PandasAnalysisTool(
    name="Pandas Analyse Tool",
    description="Voert basis- en geavanceerde analyses uit op een pandas DataFrame, zoals samenvattingen, correlatie, regressie, clustering, classificatie en anomaly detection."
)

consultant_agent = Agent(
    role="Performance Analyst",
    goal="Analyseert de verschillen t.o.v. vorige periodes, detecteert afwijkingen of optimalisatiekansen, en voert geavanceerde statistische en machine learning analyses uit.",
    backstory=(
        "Een ervaren bedrijfsanalist die patronen herkent in productprestaties. "
        "Kan afwijkingen signaleren, zwakke plekken benoemen en suggesties doen voor verbeteracties. "
        "Maakt gebruik van data van de Analist om businessinzicht te creëren en gebruikt geavanceerde statistiek en ML."
    ),
    tools=[pandas_tool],
    llm=gpt4
)



class ForecastingArgs(BaseModel):
    data: dict
    periods: int = 3
    freq: str = "M"

class ForecastingTool(BaseTool):
    args_schema: Type[ForecastingArgs] = ForecastingArgs
    model_config = {"arbitrary_types_allowed": True}
    def _run(self, data: dict, periods: int = 3, freq: str = "M"):
        print("DEBUG: ForecastingTool _run called")
        print(f"DEBUG: data={{'keys': list(data.keys())}}, periods={periods}, freq={freq}")
        try:
            df = pd.DataFrame(data)
            m = Prophet()
            m.fit(df)
            future = m.make_future_dataframe(periods=periods, freq=freq)
            forecast = m.predict(future)
            return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(periods).to_dict()
        except Exception as e:
            print("DEBUG: ForecastingTool except-blok bereikt!")
            print("ERROR in ForecastingTool:", e)
            traceback.print_exc()
            return f"TOOL ERROR: {e}"
        
forecasting_tool = ForecastingTool(
    name="Forecasting Tool",
    description="Voert geavanceerde tijdreeksvoorspellingen uit met Prophet."
)

forecaster_agent = Agent(
    role="Forecasting Analyst",
    goal="Voert diepgaande analyses en nauwkeurige voorspellingen uit op basis van historische data, en levert bruikbare inzichten voor strategische besluitvorming.",
    backstory=(
        "Een expert in data science en statistiek. Combineert klassieke analyse met moderne forecasting-technieken zoals Prophet. "
        "Werkt nauw samen met de Analist en Consultant om niet alleen het verleden te verklaren, maar ook de toekomst te voorspellen."
    ),
    tools=[forecasting_tool],
    llm=gpt4
)

# Verwijder voorbeeld/testcode onderaan het bestand 