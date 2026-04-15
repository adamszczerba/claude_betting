def get_implied_probability(odd: float):
    """Convert decimal odds to implied probability."""
    if odd <= 1.0:
        raise ValueError("Odds must be greater than 1.0")
    return 1.0 / odd

def remove_overround(probability: float, removal_method: str):

    if removal_method == 'normalize':
        pass
    elif removal_method == 'shin':
        pass
    elif removal_method == 'power':
        pass

"""
3) Ważenie bukmacherów (nie każdy jest równy)

Nie dawaj wszystkim równej wagi.

Wagi możesz budować na podstawie:

historycznej dokładności (closing line accuracy)
limitów (im większe limity, tym większa waga)
częstotliwości aktualizacji (kto szybciej reaguje)
odchylenia od rynku (kto często „odjeżdża” jest mniej wiarygodny)
"""

"""
6) Wykrywanie nieprawidłowości: porównuj do fair odds

Po zbudowaniu konsensusu wyliczasz fair odds:

fair_odds = 1 / p_konsensus

I dopiero wtedy liczysz value:

value = (kurs_bukmachera / fair_odds) - 1

Próg typu:

+1.5% dla rynków głównych
+3–5% dla niszowych
7) Najlepsza wersja konsensusu (praktyczna)

Jeśli chcesz podejście, które działa w realu:

Konsensus = median(a nie średnia) z implied probabilities po zdjęciu marży, ważona jakością bukmachera.

Median jest odporna na outliery (np. jeden bukmacher z błędem).
"""