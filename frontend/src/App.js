import React, { useState, useEffect } from 'react';

function App() {
  const [leaderboard, setLeaderboard] = useState([]);
  const [status, setStatus] = useState('Connecting...');

  useEffect(() => {
    // Connected to FastAPI WebSocket on port 8080
    const ws = new WebSocket('ws://localhost:8080/ws/leaderboard');

    ws.onopen = () => setStatus('Live');
    ws.onclose = () => setStatus('Disconnected');
    
    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      setLeaderboard(data);
    };

    return () => ws.close();
  }, []);

  return (
    <div className="min-h-screen bg-gray-900 text-white p-8 font-sans">
      <header className="flex justify-between items-center mb-8 border-b border-gray-700 pb-4">
        <div>
          <h1 className="text-3xl font-bold tracking-wider text-blue-400">IICPC Live Telemetry</h1>
          <p className="text-gray-400 text-sm mt-1">Real-time Trading Engine Analytics</p>
        </div>
        <div className="flex items-center gap-2 bg-gray-800 px-4 py-2 rounded-full border border-gray-700">
          <span className={`h-3 w-3 rounded-full ${status === 'Live' ? 'bg-green-500 animate-pulse' : 'bg-red-500'}`}></span>
          <span className="text-sm text-gray-300 font-mono">SYS.STATE: {status}</span>
        </div>
      </header>

      <div className="overflow-x-auto rounded-xl shadow-2xl border border-gray-700">
        <table className="w-full text-left font-mono text-sm">
          <thead className="bg-gray-800 text-gray-400 uppercase tracking-wider">
            <tr>
              <th className="p-4">Rank</th>
              <th className="p-4">Team Engine</th>
              <th className="p-4 text-right">Score</th>
              <th className="p-4 text-right">Accuracy</th>
              <th className="p-4 text-right">TPS</th>
              <th className="p-4 text-right">p50 (ms)</th>
              <th className="p-4 text-right">p90 (ms)</th>
              <th className="p-4 text-right">p99 (ms)</th>
            </tr>
          </thead>
          <tbody className="bg-gray-900 divide-y divide-gray-800">
            {leaderboard.length === 0 && (
              <tr><td colSpan="8" className="p-12 text-center text-gray-500 italic">Awaiting telemetry streams from bot-fleet...</td></tr>
            )}
            {leaderboard.map((team, index) => (
              <tr key={team.team_id} className="hover:bg-gray-800 transition-colors">
                <td className="p-4 font-bold text-blue-400">#{index + 1}</td>
                <td className="p-4 text-gray-200">{team.team_id}</td>
                <td className="p-4 text-right font-bold text-green-400">{team.score.toFixed(1)}</td>
                <td className="p-4 text-right text-gray-300">{team.accuracy}%</td>
                <td className="p-4 text-right text-purple-400">{team.tps}</td>
                <td className="p-4 text-right text-gray-400">{team.p50}</td>
                <td className="p-4 text-right text-yellow-400">{team.p90}</td>
                <td className="p-4 text-right text-red-400">{team.p99}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default App;