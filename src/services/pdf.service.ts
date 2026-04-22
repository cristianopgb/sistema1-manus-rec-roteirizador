import jsPDF from 'jspdf'
import autoTable from 'jspdf-autotable'
import { ManifestoComFrete, ManifestoItemRoteirizacao, ManifestoRoteirizacaoDetalhe } from '@/types'

export async function gerarPdfManifesto(manifesto: ManifestoComFrete): Promise<void> {
  const doc = new jsPDF({ orientation: 'portrait', unit: 'mm', format: 'a4' })
  const pageW = doc.internal.pageSize.getWidth()
  const margin = 14

  // ─── CABEÇALHO ──────────────────────────────────────────────────────────
  doc.setFillColor(30, 64, 175) // brand-700
  doc.rect(0, 0, pageW, 28, 'F')

  doc.setTextColor(255, 255, 255)
  doc.setFontSize(14)
  doc.setFont('helvetica', 'bold')
  doc.text('MANIFESTO DE ENTREGA', margin, 11)

  doc.setFontSize(9)
  doc.setFont('helvetica', 'normal')
  doc.text(`Nº ${manifesto.numero_manifesto}`, margin, 18)
  doc.text(`Emitido em: ${new Date().toLocaleString('pt-BR')}`, margin, 23)

  // Status aprovado
  if (manifesto.aprovado) {
    doc.setFillColor(34, 197, 94)
    doc.roundedRect(pageW - margin - 28, 8, 28, 10, 2, 2, 'F')
    doc.setFontSize(8)
    doc.setFont('helvetica', 'bold')
    doc.text('APROVADO', pageW - margin - 14, 14.5, { align: 'center' })
  }

  // ─── DADOS DO VEÍCULO ────────────────────────────────────────────────────
  doc.setTextColor(0, 0, 0)
  doc.setFontSize(10)
  doc.setFont('helvetica', 'bold')
  doc.text('DADOS DO VEÍCULO / ROTA', margin, 36)

  const dadosVeiculo = [
    ['Tipo de Veículo', manifesto.veiculo_tipo || '—', 'Placa', manifesto.placa || '—'],
    ['Motorista', manifesto.motorista || '—', 'Região', manifesto.regiao || '—'],
    ['Capacidade', `${(manifesto.capacidade_peso_kg || 0).toLocaleString('pt-BR')} kg`, 'Ocupação', `${(manifesto.ocupacao_percentual || 0).toFixed(1)}%`],
    ['Peso Total', `${(manifesto.total_peso_kg || 0).toLocaleString('pt-BR')} kg`, 'KM Estimado', `${(manifesto.km_estimado || 0).toLocaleString('pt-BR')} km`],
  ]

  autoTable(doc, {
    startY: 39,
    body: dadosVeiculo,
    theme: 'grid',
    styles: { fontSize: 8, cellPadding: 2 },
    columnStyles: {
      0: { fontStyle: 'bold', fillColor: [241, 245, 249], cellWidth: 35 },
      1: { cellWidth: 55 },
      2: { fontStyle: 'bold', fillColor: [241, 245, 249], cellWidth: 30 },
      3: { cellWidth: 55 },
    },
    margin: { left: margin, right: margin },
  })

  // ─── FRETE MÍNIMO ANTT ───────────────────────────────────────────────────
  const yAposVeiculo = (doc as any).lastAutoTable.finalY + 4

  if (manifesto.frete_minimo_antt != null) {
    doc.setFillColor(245, 243, 255)
    doc.setDrawColor(139, 92, 246)
    doc.roundedRect(margin, yAposVeiculo, pageW - margin * 2, 12, 2, 2, 'FD')

    doc.setFontSize(9)
    doc.setFont('helvetica', 'bold')
    doc.setTextColor(109, 40, 217)
    doc.text('FRETE MÍNIMO ANTT (Tabela Vigente):', margin + 3, yAposVeiculo + 5)

    doc.setFontSize(11)
    doc.text(
      `R$ ${manifesto.frete_minimo_antt.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}`,
      pageW - margin - 3,
      yAposVeiculo + 5,
      { align: 'right' }
    )

    if (manifesto.tipo_carga_antt) {
      doc.setFontSize(7)
      doc.setFont('helvetica', 'normal')
      doc.text(
        `Tipo: ${manifesto.tipo_carga_antt} · Eixos: ${manifesto.num_eixos || '—'} · CCD: R$/km ${manifesto.coef_ccd || '—'} · CC: R$ ${manifesto.coef_cc || '—'}`,
        margin + 3,
        yAposVeiculo + 10
      )
    }
  }

  // ─── AGENDAMENTOS ────────────────────────────────────────────────────────
  const yAposAntt = manifesto.frete_minimo_antt != null
    ? yAposVeiculo + 16
    : yAposVeiculo

  if (manifesto.agendamentos && manifesto.agendamentos.length > 0) {
    doc.setTextColor(0, 0, 0)
    doc.setFontSize(10)
    doc.setFont('helvetica', 'bold')
    doc.text('AGENDAMENTOS', margin, yAposAntt + 4)

    autoTable(doc, {
      startY: yAposAntt + 7,
      head: [['Destinatário', 'Cidade', 'Data Agenda', 'Hora', 'Documento']],
      body: manifesto.agendamentos.map((a) => [
        a.destinatario || '—',
        `${a.cidade || '—'} / ${a.uf || '—'}`,
        a.data_agenda ? new Date(a.data_agenda).toLocaleDateString('pt-BR') : '—',
        a.hora_agenda || '—',
        a.nro_documento || '—',
      ]),
      theme: 'striped',
      headStyles: { fillColor: [251, 191, 36], textColor: [0, 0, 0], fontStyle: 'bold', fontSize: 8 },
      styles: { fontSize: 8, cellPadding: 2 },
      margin: { left: margin, right: margin },
    })
  }

  // ─── ENTREGAS ────────────────────────────────────────────────────────────
  const yAposAgend = (doc as any).lastAutoTable?.finalY + 6 || yAposAntt + 6

  doc.setTextColor(0, 0, 0)
  doc.setFontSize(10)
  doc.setFont('helvetica', 'bold')
  doc.text(`ENTREGAS (${manifesto.entregas.length})`, margin, yAposAgend)

  autoTable(doc, {
    startY: yAposAgend + 3,
    head: [['Seq', 'Destinatário', 'Cidade / UF', 'Documentos / NFs', 'Peso (kg)', 'Valor (R$)', 'Data Limite', 'Folga']],
    body: manifesto.entregas.map((e) => [
      e.sequencia,
      e.destinatario || '—',
      `${e.cidade || '—'} / ${e.uf || '—'}`,
      e.lista_nfs?.join(', ') || e.nro_documento || '—',
      (e.peso_kg || 0).toLocaleString('pt-BR', { minimumFractionDigits: 1 }),
      e.valor_mercadoria
        ? e.valor_mercadoria.toLocaleString('pt-BR', { minimumFractionDigits: 2 })
        : '—',
      e.data_limite_entrega
        ? new Date(e.data_limite_entrega).toLocaleDateString('pt-BR')
        : '—',
      e.folga_dias != null ? `${e.folga_dias.toFixed(0)}d` : '—',
    ]),
    theme: 'striped',
    headStyles: { fillColor: [30, 64, 175], textColor: [255, 255, 255], fontStyle: 'bold', fontSize: 7.5 },
    styles: { fontSize: 7.5, cellPadding: 2 },
    columnStyles: {
      0: { cellWidth: 8, halign: 'center' },
      1: { cellWidth: 38 },
      2: { cellWidth: 28 },
      3: { cellWidth: 35 },
      4: { cellWidth: 18, halign: 'right' },
      5: { cellWidth: 22, halign: 'right' },
      6: { cellWidth: 20, halign: 'center' },
      7: { cellWidth: 12, halign: 'center' },
    },
    margin: { left: margin, right: margin },
    didParseCell: (data) => {
      // Colorir linha de agendamentos
      if (data.section === 'body') {
        const entrega = manifesto.entregas[data.row.index]
        if (entrega?.agendada) {
          data.cell.styles.fillColor = [254, 249, 195]
        }
        // Colorir folga crítica
        if (data.column.index === 7 && entrega?.status_folga === 'urgente') {
          data.cell.styles.textColor = [220, 38, 38]
          data.cell.styles.fontStyle = 'bold'
        }
      }
    },
  })

  // ─── TOTAIS ──────────────────────────────────────────────────────────────
  const yFinal = (doc as any).lastAutoTable.finalY + 4

  autoTable(doc, {
    startY: yFinal,
    body: [
      [
        `Total de Entregas: ${manifesto.entregas.length}`,
        `Peso Total: ${(manifesto.total_peso_kg || 0).toLocaleString('pt-BR', { minimumFractionDigits: 1 })} kg`,
        `Ocupação: ${(manifesto.ocupacao_percentual || 0).toFixed(1)}%`,
        manifesto.frete_minimo_antt != null
          ? `Frete Mínimo ANTT: R$ ${manifesto.frete_minimo_antt.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}`
          : '',
      ],
    ],
    theme: 'grid',
    styles: { fontSize: 8, fontStyle: 'bold', fillColor: [241, 245, 249], cellPadding: 3 },
    margin: { left: margin, right: margin },
  })

  // ─── RODAPÉ ──────────────────────────────────────────────────────────────
  const pageCount = doc.getNumberOfPages()
  for (let i = 1; i <= pageCount; i++) {
    doc.setPage(i)
    doc.setFontSize(7)
    doc.setTextColor(150)
    doc.setFont('helvetica', 'normal')
    doc.text(
      `REC Transportes · Manifesto ${manifesto.numero_manifesto} · Pág. ${i}/${pageCount}`,
      pageW / 2,
      doc.internal.pageSize.getHeight() - 6,
      { align: 'center' }
    )
  }

  // ─── DOWNLOAD ────────────────────────────────────────────────────────────
  doc.save(`manifesto_${manifesto.numero_manifesto}_${new Date().toISOString().slice(0, 10)}.pdf`)
}

export async function gerarPdfManifestoOperacional(
  manifesto: ManifestoRoteirizacaoDetalhe,
  itens: ManifestoItemRoteirizacao[],
  contexto: { filialNome?: string | null; dataRodada?: string | null } = {},
): Promise<void> {
  const doc = new jsPDF({ orientation: 'portrait', unit: 'mm', format: 'a4' })
  const pageW = doc.internal.pageSize.getWidth()
  const margin = 14
  const dataEmissao = new Date()

  doc.setFillColor(30, 64, 175)
  doc.rect(0, 0, pageW, 28, 'F')
  doc.setTextColor(255, 255, 255)
  doc.setFontSize(14)
  doc.setFont('helvetica', 'bold')
  doc.text('MANIFESTO OPERACIONAL', margin, 11)
  doc.setFontSize(9)
  doc.setFont('helvetica', 'normal')
  doc.text(`Manifesto: ${manifesto.manifesto_id}`, margin, 18)
  doc.text(`Emissão: ${dataEmissao.toLocaleString('pt-BR')}`, margin, 23)

  doc.setTextColor(0, 0, 0)
  doc.setFontSize(10)
  doc.setFont('helvetica', 'bold')
  doc.text('RESUMO DO MANIFESTO', margin, 36)

  autoTable(doc, {
    startY: 39,
    theme: 'grid',
    styles: { fontSize: 8, cellPadding: 2 },
    body: [
      ['Filial', contexto.filialNome || '—', 'Data da rodada', contexto.dataRodada || '—'],
      ['Veículo / Perfil', manifesto.veiculo_perfil || manifesto.veiculo_tipo || '—', 'Qtd. eixos', String(manifesto.qtd_eixos ?? '—')],
      ['Peso total', `${manifesto.peso_total.toLocaleString('pt-BR')} kg`, 'KM total', `${manifesto.km_total.toLocaleString('pt-BR')} km`],
      ['Ocupação', `${manifesto.ocupacao.toFixed(1)}%`, 'Frete mínimo', `R$ ${manifesto.frete_minimo.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}`],
      ['Qtd. entregas', String(manifesto.qtd_entregas), 'Qtd. clientes', String(manifesto.qtd_clientes)],
    ],
    columnStyles: {
      0: { fontStyle: 'bold', fillColor: [241, 245, 249], cellWidth: 35 },
      1: { cellWidth: 55 },
      2: { fontStyle: 'bold', fillColor: [241, 245, 249], cellWidth: 35 },
      3: { cellWidth: 45 },
    },
    margin: { left: margin, right: margin },
  })

  const yAposResumo = (doc as any).lastAutoTable.finalY + 6
  doc.setFontSize(10)
  doc.setFont('helvetica', 'bold')
  doc.text(`ITENS (${itens.length})`, margin, yAposResumo)

  const itensAgendados = itens.filter((item) => {
    const extra = item as unknown as Record<string, unknown>
    return Boolean(item.inicio_entrega || item.fim_entrega || extra.data_agenda || extra.janela)
  })

  autoTable(doc, {
    startY: yAposResumo + 3,
    theme: 'striped',
    headStyles: { fillColor: [30, 64, 175], textColor: [255, 255, 255], fontSize: 8 },
    styles: { fontSize: 7.5, cellPadding: 2 },
    head: [['Seq', 'Documento', 'Destino', 'Cidade/UF', 'Janela']],
    body: itens.map((item) => [
      item.sequencia,
      item.nro_documento || '—',
      item.destinatario || '—',
      `${item.cidade || '—'}/${item.uf || '—'}`,
      `${item.inicio_entrega || '—'} - ${item.fim_entrega || '—'}`,
    ]),
    margin: { left: margin, right: margin },
    columnStyles: {
      0: { cellWidth: 12 },
      1: { cellWidth: 34 },
      2: { cellWidth: 58 },
      3: { cellWidth: 30 },
      4: { cellWidth: 38 },
    },
  })

  let cursorY = (doc as any).lastAutoTable.finalY + 6
  if (cursorY > 250) {
    doc.addPage()
    cursorY = 20
  }

  doc.setFontSize(10)
  doc.setFont('helvetica', 'bold')
  doc.text('ROMANEIO / RESUMO OPERACIONAL', margin, cursorY)
  autoTable(doc, {
    startY: cursorY + 3,
    theme: 'grid',
    styles: { fontSize: 7.5, cellPadding: 1.6 },
    body: [
      ['Linha / rota', manifesto.tipo_manifesto || manifesto.manifesto_id || '—', 'Remetente', '—'],
      ['N. Fiscal(s)/Data', '—', 'Destinatário', itens[0]?.destinatario || '—'],
      ['Cidade', itens[0] ? `${itens[0].cidade || '—'} / ${itens[0].uf || '—'}` : '—', 'Doc CTRC / documento', itens[0]?.nro_documento || '—'],
      ['Peso bruto', `${manifesto.peso_total.toLocaleString('pt-BR')} kg`, 'Peso KG', `${manifesto.peso_total.toLocaleString('pt-BR')} kg`],
      ['Valor da mercadoria', '—', 'Tipo de carga', manifesto.tipo_manifesto || '—'],
      ['Data chegada', '—', 'Data descarga', '—'],
      ['Senha do SAR', '—', 'Atendente', '—'],
    ],
    columnStyles: {
      0: { fontStyle: 'bold', fillColor: [241, 245, 249], cellWidth: 35 },
      1: { cellWidth: 55 },
      2: { fontStyle: 'bold', fillColor: [241, 245, 249], cellWidth: 35 },
      3: { cellWidth: 45 },
    },
    margin: { left: margin, right: margin },
  })

  cursorY = (doc as any).lastAutoTable.finalY + 6
  if (itensAgendados.length > 0) {
    if (cursorY > 242) {
      doc.addPage()
      cursorY = 20
    }
    doc.setFontSize(10)
    doc.setFont('helvetica', 'bold')
    doc.text(`CARGAS AGENDADAS (${itensAgendados.length})`, margin, cursorY)
    autoTable(doc, {
      startY: cursorY + 3,
      theme: 'striped',
      headStyles: { fillColor: [180, 83, 9], textColor: [255, 255, 255], fontSize: 8 },
      styles: { fontSize: 7, cellPadding: 1.5 },
      head: [['CTE / Doc', 'Destinatário', 'Cidade', 'UF', 'Data', 'Hora', 'Info']],
      body: itensAgendados.map((item) => {
        const extra = item as unknown as Record<string, unknown>
        return [
          item.nro_documento || '—',
          item.destinatario || '—',
          item.cidade || '—',
          item.uf || '—',
          String(extra.data_agenda ?? '—'),
          `${item.inicio_entrega || '—'} - ${item.fim_entrega || '—'}`,
          String(extra.janela ?? extra.info_agendamento ?? 'Agendada'),
        ]
      }),
      margin: { left: margin, right: margin },
      columnStyles: {
        0: { cellWidth: 22 },
        1: { cellWidth: 44 },
        2: { cellWidth: 26 },
        3: { cellWidth: 10 },
        4: { cellWidth: 20 },
        5: { cellWidth: 24 },
      },
    })
  }

  const pageCount = doc.getNumberOfPages()
  for (let i = 1; i <= pageCount; i++) {
    doc.setPage(i)
    doc.setFontSize(7)
    doc.setTextColor(150)
    doc.text(
      `Manifesto ${manifesto.manifesto_id} · Pág. ${i}/${pageCount}`,
      pageW / 2,
      doc.internal.pageSize.getHeight() - 6,
      { align: 'center' },
    )
  }

  doc.save(`manifesto_${manifesto.manifesto_id}_${new Date().toISOString().slice(0, 10)}.pdf`)
  console.log('[PDF] manifesto exportado:', manifesto.manifesto_id)
  console.log('[PDF] cargas agendadas no manifesto:', itensAgendados.length)
}
